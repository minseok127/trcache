/**
 * @file   benchmark.c
 * @brief  A production-level performance benchmark for the trcache library.
 *
 * This program implements a fully partitioned measurement system to eliminate
 * contention and accurately reflect trcache's performance. Each symbol uses
 * an independent circular buffer for timestamps, preventing both software
 * and hardware (false sharing) contention between threads. Latency is
 * measured with nanosecond precision using HDR Histograms to analyze the
 * full distribution, including critical tail latencies. The benchmark is
 * designed for long-duration runs, ensuring stability and data consistency.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdatomic.h>
#include <math.h>
#include <sched.h>
#include <ctype.h>

#include "trcache.h"
#include "utils/tsc_clock.h"
#include "hdr_histogram.h"

#define PER_SYMBOL_BUFFER_SIZE (1 * 10 * 1000) // 10K trades per symbol
#define TICK_CANDLE_INTERVAL 100
#define TIME_CANDLE_INTERVAL_MS 60000
#define CACHE_LINE_SIZE 64

/**
 * @brief Defines the market simulation scenarios.
 */
typedef enum {
	SCENARIO_HFT,
	SCENARIO_FLASH_CRASH,
	SCENARIO_GAPPING,
} scenario_type;

/**
 * @brief Defines which candle types are active for the benchmark run.
 */
typedef enum {
	MODE_TIME_ONLY,
	MODE_TICK_ONLY,
	MODE_BOTH
} candle_mode_type;

/**
 * @brief A 16-byte structure for timestamp data, protected by a seqlock.
 */
typedef struct {
	_Atomic unsigned int sequence;
	uint64_t creation_ns;
	uint64_t full_trade_id; // Per-symbol trade id
} timestamp_entry;


/**
 * @brief Benchmark configuration parameters, set via command-line arguments.
 */
typedef struct {
	int num_worker_threads;
	int num_feeder_threads;
	int num_symbols;
	int duration_sec;
	int warmup_sec;
	int start_cpu_core;
	scenario_type scenario;
	candle_mode_type candle_mode;
} benchmark_config;

/**
 * @brief A snapshot of performance metrics for a single time interval.
 */
typedef struct {
	int timestamp;
	double throughput;
	double p99_tick_us;
	double p99_time_us;
	double p99_closing_us;
	long discarded_samples;
} periodic_stats;

/**
 * @brief Data shared across all benchmark threads.
 */
typedef struct {
	struct trcache *cache;
	const benchmark_config *config;
	_Atomic bool should_stop;
	_Atomic bool warmup_done;
	_Atomic long total_trades_fed;
	uint64_t* symbol_trade_id_counters;
	int *symbol_ids;
	timestamp_entry** trade_creation_times_per_symbol;
	_Atomic uint64_t* time_closing_trade_ids;
	_Atomic uint64_t* last_known_closing_trade_id;
	_Atomic long discarded_sample_count;

	struct hdr_histogram* tick_latency_hist;
	struct hdr_histogram* time_latency_hist;
	struct hdr_histogram* closing_trade_latency_hist;
	pthread_mutex_t hist_mutex;

	periodic_stats* time_series_stats;
} shared_context;

/**
 * @brief Arguments for each individual feeder thread.
 */
typedef struct {
	shared_context *s_ctx;
	int feeder_id;
} feeder_thread_args;

// --- Function Prototypes ---
void* dummy_sync_flush(trcache *c, trcache_candle_batch *b, void *flush_ctx);
double generate_normal_random();
void* feeder_thread_main(void *arg);
void* querier_thread_main(void *arg);
void* monitor_thread_main(void *arg);
void print_final_summary(
	const benchmark_config *config, shared_context *ctx);
void print_time_series_data(
	const benchmark_config *config, shared_context *ctx);
void set_thread_affinity(int core_id);
int parse_duration(const char* str);
const char* scenario_to_string(scenario_type s);
const char* candle_mode_to_string(candle_mode_type m);

DEFINE_TIME_CANDLE_OPS(1m, TIME_CANDLE_INTERVAL_MS);
DEFINE_TICK_CANDLE_OPS(100t, TICK_CANDLE_INTERVAL);

/**
 * @brief   Main entry point for the benchmark program.
 */
int main(int argc, char *argv[])
{
	if (argc < 8) {
		fprintf(
			stderr,
			"Usage: %s <workers> <symbols> <duration> <warmup> "
			"<scenario> <candle_mode> <start_core>\n",
			argv[0]);
		fprintf(stderr, "  Duration format: e.g., 1h, 30m, 10s\n");
		fprintf(stderr, "  Scenario: 0=HFT, 1=FlashCrash, 2=Gapping\n");
		fprintf(
			stderr, "  Candle Mode: 0=TimeOnly, 1=TickOnly, 2=Both\n");
		return 1;
	}

	benchmark_config config = {
		.num_worker_threads = atoi(argv[1]),
		.num_symbols = atoi(argv[2]),
		.duration_sec = parse_duration(argv[3]),
		.warmup_sec = parse_duration(argv[4]),
		.scenario = (scenario_type)atoi(argv[5]),
		.candle_mode = (candle_mode_type)atoi(argv[6]),
		.start_cpu_core = atoi(argv[7]),
		.num_feeder_threads = 4
	};
	if (config.duration_sec <= 0 || config.warmup_sec < 0) return 1;

	// --- trcache Initialization ---
	trcache_candle_config time_candles[] = {
		{ .threshold.interval_ms = TIME_CANDLE_INTERVAL_MS,
		  .update_ops = ops_1m,
		  .flush_ops = { .flush = dummy_sync_flush } }
	};
	trcache_candle_config tick_candles[] = {
		{ .threshold.num_ticks = TICK_CANDLE_INTERVAL,
		  .update_ops = ops_100t,
		  .flush_ops = { .flush = dummy_sync_flush } }
	};

	struct trcache_init_ctx init_ctx = {
		.num_worker_threads = config.num_worker_threads,
		.batch_candle_count_pow2 = 10, .cached_batch_count_pow2 = 3,
	};
	if (config.candle_mode == MODE_TIME_ONLY ||
		config.candle_mode == MODE_BOTH) {
		init_ctx.candle_types[CANDLE_TIME_BASE] = time_candles;
		init_ctx.num_candle_types[CANDLE_TIME_BASE] = 1;
	}
	if (config.candle_mode == MODE_TICK_ONLY ||
		config.candle_mode == MODE_BOTH) {
		init_ctx.candle_types[CANDLE_TICK_BASE] = tick_candles;
		init_ctx.num_candle_types[CANDLE_TICK_BASE] = 1;
	}

	struct trcache *cache = trcache_init(&init_ctx);
	if (!cache) {
		fprintf(stderr, "Failed to initialize trcache\n"); return 1;
	}

	int *symbol_ids = malloc(sizeof(int) * config.num_symbols);
	for (int i = 0; i < config.num_symbols; ++i) {
		char symbol_str[16]; snprintf(symbol_str, 16, "SYM%d", i);
		symbol_ids[i] = trcache_register_symbol(cache, symbol_str);
	}

	shared_context ctx = {
		.cache = cache, .config = &config, .symbol_ids = symbol_ids
	};
	atomic_init(&ctx.should_stop, false);
	atomic_init(&ctx.warmup_done, false);
	atomic_init(&ctx.total_trades_fed, 0);
	atomic_init(&ctx.discarded_sample_count, 0);
	pthread_mutex_init(&ctx.hist_mutex, NULL);
	ctx.symbol_trade_id_counters =
		calloc(config.num_symbols, sizeof(uint64_t));
	ctx.trade_creation_times_per_symbol =
		malloc(config.num_symbols * sizeof(timestamp_entry*));
	for (int i = 0; i < config.num_symbols; i++) {
		if (posix_memalign(
				(void**)&ctx.trade_creation_times_per_symbol[i],
				CACHE_LINE_SIZE,
				PER_SYMBOL_BUFFER_SIZE * sizeof(timestamp_entry)) != 0) {
			fprintf(stderr,
				"Failed to allocate aligned memory for symbol %d\n", i);
			return 1;
		}
	}
	ctx.time_closing_trade_ids = calloc(
		config.num_symbols, sizeof(_Atomic uint64_t));
	ctx.last_known_closing_trade_id = calloc(
		config.num_symbols, sizeof(_Atomic uint64_t));
	hdr_histogram_init(1, 1000000000, 3, &ctx.tick_latency_hist);
	hdr_histogram_init(1, 1000000000, 3, &ctx.time_latency_hist);
	hdr_histogram_init(1, 1000000000, 3, &ctx.closing_trade_latency_hist);
	ctx.time_series_stats =
		calloc(config.duration_sec, sizeof(periodic_stats));

	// --- Thread Creation & Execution ---
	pthread_t feeders[config.num_feeder_threads];
	feeder_thread_args f_args[config.num_feeder_threads];
	pthread_t querier, monitor;
	printf("Starting benchmark...\n");
	for (int i = 0; i < config.num_feeder_threads; ++i) {
		f_args[i] = (feeder_thread_args){ .s_ctx = &ctx, .feeder_id = i };
		pthread_create(&feeders[i], NULL, feeder_thread_main, &f_args[i]);
	}
	pthread_create(&querier, NULL, querier_thread_main, &ctx);
	pthread_create(&monitor, NULL, monitor_thread_main, &ctx);

	printf("Warm-up for %d seconds...\n", config.warmup_sec);
	sleep(config.warmup_sec);
	atomic_store(&ctx.warmup_done, true);
	printf("Measurement for %d seconds...\n", config.duration_sec);
	sleep(config.duration_sec);
	atomic_store(&ctx.should_stop, true);

	for (int i = 0; i < config.num_feeder_threads; ++i) {
		pthread_join(feeders[i], NULL);
	}
	pthread_join(querier, NULL);
	pthread_join(monitor, NULL);

	print_final_summary(&config, &ctx);
	print_time_series_data(&config, &ctx);

	// --- Cleanup ---
	trcache_destroy(cache);
	free(symbol_ids);
	free(ctx.symbol_trade_id_counters);
	for (int i = 0; i < config.num_symbols; i++) {
		free(ctx.trade_creation_times_per_symbol[i]);
	}
	free(ctx.trade_creation_times_per_symbol);
	free(ctx.time_closing_trade_ids);
	free(ctx.last_known_closing_trade_id);
	hdr_histogram_close(ctx.tick_latency_hist);
	hdr_histogram_close(ctx.time_latency_hist);
	hdr_histogram_close(ctx.closing_trade_latency_hist);
	free(ctx.time_series_stats);
	pthread_mutex_destroy(&ctx.hist_mutex);

	return 0;
}

// --- Thread Implementations & Helpers ---

/**
 * @brief   Sets the CPU affinity for the calling thread.
 * @param   core_id The CPU core to pin the thread to.
 */
void set_thread_affinity(int core_id)
{
	if (core_id < 0) return;
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	if (pthread_setaffinity_np(
		pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
		perror("pthread_setaffinity_np");
	}
}

/**
 * @brief   Generates a normally distributed random number.
 * @return  A random number from a standard normal distribution.
 */
double generate_normal_random()
{
	double u1 = (double)rand() / RAND_MAX;
	double u2 = (double)rand() / RAND_MAX;
	return sqrt(-2.0 * log(u1)) * cos(2.0 * M_PI * u2);
}

/**
 * @brief   Feeder thread that generates and ingests trade data.
 */
void* feeder_thread_main(void *arg)
{
	feeder_thread_args *f_args = (feeder_thread_args *)arg;
	shared_context *ctx = f_args->s_ctx;
	const benchmark_config *config = ctx->config;
	int core_id = config->start_cpu_core;

	if (core_id >= 0) {
		set_thread_affinity(core_id + f_args->feeder_id);
	}

	double price = 150.0 + (f_args->feeder_id * 10);
	double mu = 0.0000001, sigma = 0.0001, dt = 1.0;
	uint64_t last_timestamp_ms[config->num_symbols];
	memset(last_timestamp_ms, 0, sizeof(last_timestamp_ms));

	int num_feeders = config->num_feeder_threads;
	int symbols_per_feeder = config->num_symbols / num_feeders;
	int start_idx = f_args->feeder_id * symbols_per_feeder;
	int end_idx = start_idx + symbols_per_feeder;
	if (f_args->feeder_id == num_feeders - 1) {
		end_idx = config->num_symbols;
	}

	struct timespec now;
	time_t start_time = time(NULL);

	while (!atomic_load(&ctx->should_stop)) {
		time_t elapsed_sec = time(NULL) - start_time;
		if (config->scenario == SCENARIO_FLASH_CRASH) {
			if ((elapsed_sec % 60) > 5) usleep(1000);
		} else if (config->scenario == SCENARIO_GAPPING) {
			if (rand() % 100 < 5) {
				usleep(50000 + (rand() % 50000));
				price += ((double)rand() / RAND_MAX - 0.5) * 5.0;
			}
		}

		for (int i = start_idx; i < end_idx; ++i) {
			price *= exp((mu-0.5*sigma*sigma)*dt + sigma*sqrt(dt)*generate_normal_random());
			if (price <= 0) price = 0.01;

			uint64_t trade_id = ctx->symbol_trade_id_counters[i]++;
			uint64_t buffer_idx = trade_id % PER_SYMBOL_BUFFER_SIZE;
			timestamp_entry* symbol_buffer = ctx->trade_creation_times_per_symbol[i];

			clock_gettime(CLOCK_MONOTONIC, &now);
			
			timestamp_entry* entry = &symbol_buffer[buffer_idx];
			atomic_fetch_add(&entry->sequence, 1);
			atomic_thread_fence(memory_order_release);
			entry->creation_ns =
				(uint64_t)now.tv_sec * 1000000000 + now.tv_nsec;
			entry->full_trade_id = trade_id;
			atomic_thread_fence(memory_order_release);
			atomic_fetch_add(&entry->sequence, 1);

			uint64_t current_ts_ms =
				(uint64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;

			if (last_timestamp_ms[i] > 0 &&
				(current_ts_ms / TIME_CANDLE_INTERVAL_MS) >
				(last_timestamp_ms[i] / TIME_CANDLE_INTERVAL_MS)) {
				atomic_store(&ctx->time_closing_trade_ids[i], trade_id);
			}
			last_timestamp_ms[i] = current_ts_ms;

			if ((trade_id + 1) % TICK_CANDLE_INTERVAL == 0) {
				atomic_store(&ctx->last_known_closing_trade_id[i], trade_id);
			}

			struct trcache_trade_data td = {
				.timestamp = current_ts_ms, .trade_id = trade_id,
				.price = price, .volume = (rand() % 10 == 0) ?
					(double)(rand() % 1000 + 100) : (double)(rand() % 100 + 1),
			};
			trcache_feed_trade_data(ctx->cache, &td, ctx->symbol_ids[i]);

			if (atomic_load(&ctx->warmup_done)) {
				atomic_fetch_add(&ctx->total_trades_fed, 1);
			}
		}
	}
	return NULL;
}

/**
 * @brief   Querier thread that measures end-to-end latency.
 */
void* querier_thread_main(void *arg)
{
	shared_context *ctx = (shared_context *)arg;
	const benchmark_config *config = ctx->config;
	int core_id = config->start_cpu_core;

	if (core_id >= 0) {
		set_thread_affinity(core_id + config->num_feeder_threads);
	}

	uint64_t last_processed_closing_id[config->num_symbols];
	uint64_t last_seen_time_ts[config->num_symbols];
	memset(last_processed_closing_id, 0, sizeof(last_processed_closing_id));
	memset(last_seen_time_ts, 0, sizeof(last_seen_time_ts));

	TRCACHE_DEFINE_BATCH_ON_STACK(
		batch, 1, TRCACHE_START_TIMESTAMP | TRCACHE_IS_CLOSED);
	trcache_candle_type tick_type = {.base = CANDLE_TICK_BASE, .type_idx = 0};
	trcache_candle_type time_type = {.base = CANDLE_TIME_BASE, .type_idx = 0};

	while (!atomic_load(&ctx->should_stop)) {
		int symbol_idx = rand() % config->num_symbols;
		int target_symbol_id = ctx->symbol_ids[symbol_idx];

		// --- Measure Tick Candle Latency ---
		if (config->candle_mode != MODE_TIME_ONLY) {
			uint64_t closing_trade_id =
				atomic_load(&ctx->last_known_closing_trade_id[symbol_idx]);

			if (closing_trade_id > last_processed_closing_id[symbol_idx]) {
				int ret = trcache_get_candles_by_symbol_id_and_offset(
					ctx->cache, target_symbol_id, tick_type, 0, 1, 1, &batch);

				if (ret == 0 && batch.num_candles > 0) {
					uint64_t buffer_idx =
						closing_trade_id % PER_SYMBOL_BUFFER_SIZE;
					timestamp_entry* entry_ptr =
						&ctx->trade_creation_times_per_symbol[symbol_idx][buffer_idx];
					timestamp_entry local_entry;
					unsigned int seq1, seq2;
					while (1) {
						seq1 = atomic_load_explicit(&entry_ptr->sequence,
							memory_order_acquire);
						if (seq1 & 1) {
							continue;
						}

						atomic_thread_fence(memory_order_acquire);
						local_entry.creation_ns = entry_ptr->creation_ns;
						local_entry.full_trade_id = entry_ptr->full_trade_id;
						atomic_thread_fence(memory_order_acquire);
	
						seq2 = atomic_load_explicit(&entry_ptr->sequence,
							memory_order_acquire);

						if (seq1 == seq2) {
							break;
						}
					}
					
					if (local_entry.full_trade_id == closing_trade_id &&
						atomic_load(&ctx->warmup_done)) {
						struct timespec now;
						clock_gettime(CLOCK_MONOTONIC, &now);
						uint64_t now_ns =
							(uint64_t)now.tv_sec*1000000000 + now.tv_nsec;
						uint64_t latency = now_ns - local_entry.creation_ns;
						
						pthread_mutex_lock(&ctx->hist_mutex);
						hdr_histogram_record_value(
							ctx->tick_latency_hist, latency);
						hdr_histogram_record_value(
							ctx->closing_trade_latency_hist, latency);
						pthread_mutex_unlock(&ctx->hist_mutex);
					} else {
						atomic_fetch_add(&ctx->discarded_sample_count, 1);
					}
					last_processed_closing_id[symbol_idx] = closing_trade_id;
				}
			}
		}

		// --- Measure Time Candle Latency ---
		if (config->candle_mode != MODE_TICK_ONLY) {
			int ret = trcache_get_candles_by_symbol_id_and_offset(
				ctx->cache, target_symbol_id, time_type,
				TRCACHE_START_TIMESTAMP | TRCACHE_IS_CLOSED, 1, 1, &batch);
			
			if (ret == 0 && batch.num_candles > 0 && batch.is_closed_array[0]) {
				uint64_t closed_ts = batch.start_timestamp_array[0];
				
				if (closed_ts > last_seen_time_ts[symbol_idx]) {
					uint64_t closing_trade_id =
						atomic_load(&ctx->time_closing_trade_ids[symbol_idx]);
					uint64_t buffer_idx =
						closing_trade_id % PER_SYMBOL_BUFFER_SIZE;
					timestamp_entry* entry_ptr =
						&ctx->trade_creation_times_per_symbol[symbol_idx][buffer_idx];
					timestamp_entry local_entry;
					unsigned int seq1, seq2;

					while (1) {
						seq1 = atomic_load_explicit(&entry_ptr->sequence,
							memory_order_acquire);
						if (seq1 & 1) {
							continue;
						}

						atomic_thread_fence(memory_order_acquire);
						local_entry.creation_ns = entry_ptr->creation_ns;
						local_entry.full_trade_id = entry_ptr->full_trade_id;
						atomic_thread_fence(memory_order_acquire);
	
						seq2 = atomic_load_explicit(&entry_ptr->sequence,
							memory_order_acquire);

						if (seq1 == seq2) {
							break;
						}
					}
					
					if (local_entry.full_trade_id == closing_trade_id &&
						atomic_load(&ctx->warmup_done)) {
						struct timespec now;
						clock_gettime(CLOCK_MONOTONIC, &now);
						uint64_t now_ns =
							(uint64_t)now.tv_sec*1000000000 + now.tv_nsec;
						uint64_t latency = now_ns - local_entry.creation_ns;

						pthread_mutex_lock(&ctx->hist_mutex);
						hdr_histogram_record_value(
							ctx->time_latency_hist, latency);
						hdr_histogram_record_value(
							ctx->closing_trade_latency_hist, latency);
						pthread_mutex_unlock(&ctx->hist_mutex);
					} else {
						atomic_fetch_add(&ctx->discarded_sample_count, 1);
					}
					last_seen_time_ts[symbol_idx] = closed_ts;
				}
			}
		}
		usleep(100);
	}
	return NULL;
}


/**
 * @brief   Monitor thread that periodically records performance metrics.
 */
void* monitor_thread_main(void *arg)
{
	shared_context *ctx = (shared_context *)arg;
	const benchmark_config *config = ctx->config;
	int core_id = config->start_cpu_core;

	if (core_id >= 0) {
		set_thread_affinity(core_id + config->num_feeder_threads + 1);
	}

	while (!atomic_load(&ctx->warmup_done)) sleep(1);

	long last_trade_count = 0;
	for (int i = 0; i < config->duration_sec; ++i) {
		sleep(1);
		if (atomic_load(&ctx->should_stop)) break;

		long current_trade_count = atomic_load(&ctx->total_trades_fed);
		
		periodic_stats* stats = &ctx->time_series_stats[i];
		stats->timestamp = i + 1;
		stats->throughput = (double)(current_trade_count - last_trade_count);
		last_trade_count = current_trade_count;

		pthread_mutex_lock(&ctx->hist_mutex);
		stats->p99_tick_us = (double)hdr_histogram_value_at_percentile(
			ctx->tick_latency_hist, 99.0) / 1000.0;
		stats->p99_time_us = (double)hdr_histogram_value_at_percentile(
			ctx->time_latency_hist, 99.0) / 1000.0;
		stats->p99_closing_us = (double)hdr_histogram_value_at_percentile(
			ctx->closing_trade_latency_hist, 99.0) / 1000.0;
		stats->discarded_samples =
			atomic_exchange(&ctx->discarded_sample_count, 0);
		hdr_histogram_reset(ctx->tick_latency_hist);
		hdr_histogram_reset(ctx->time_latency_hist);
		hdr_histogram_reset(ctx->closing_trade_latency_hist);
		pthread_mutex_unlock(&ctx->hist_mutex);
	}
	return NULL;
}


/**
 * @brief   Prints a final summary of the benchmark run.
 */
void print_final_summary(
	const benchmark_config *config, shared_context *ctx)
{
	long total_trades = atomic_load(&ctx->total_trades_fed);
	double throughput = (double)total_trades / config->duration_sec;
	printf("\n--- Benchmark Summary ---\n");
	printf("  Scenario: %s, Candle Mode: %s\n",
		scenario_to_string(config->scenario),
		candle_mode_to_string(config->candle_mode));
	printf(
		"  Config: %d workers, %d feeders, %d symbols, "
		"%ds duration (%ds warmup)\n",
		config->num_worker_threads, config->num_feeder_threads,
		config->num_symbols, config->duration_sec, config->warmup_sec);
	printf("-----------------------------------------------------------------\n");
	printf("  Average Ingestion Throughput: %.2f trades/sec\n", throughput);
	printf("\n--- System Status at Shutdown ---\n");
	trcache_print_total_memory_breakdown(ctx->cache);
	printf("\n");
	trcache_print_worker_distribution(ctx->cache);
	printf("-----------------------------------------------------------------\n");
}

/**
 * @brief   Prints the collected time-series data in a CSV-friendly format.
 */
void print_time_series_data(
	const benchmark_config *config, shared_context *ctx)
{
	printf("\n--- Time-Series Performance Data (1-second intervals) ---\n");
	printf(
		"%-8s %-15s %-15s %-15s %-15s %-10s\n",
		"Time(s)", "Throughput", "p99_Tick(us)", "p99_Time(us)",
		"p99_Closing(us)", "Discarded");
	printf("-----------------------------------------------------------------"
		"----------\n");
	for(int i = 0; i < config->duration_sec; ++i) {
		periodic_stats* s = &ctx->time_series_stats[i];
		if (s->timestamp == 0) continue;
		printf(
			"%-8d %-15.0f %-15.3f %-15.3f %-15.3f %-10ld\n",
			s->timestamp, s->throughput, s->p99_tick_us,
			s->p99_time_us, s->p99_closing_us, s->discarded_samples);
	}
	printf("-----------------------------------------------------------------"
		"----------\n");
}

/**
 * @brief   A dummy flush operation that does nothing.
 */
void* dummy_sync_flush(
	trcache *c, trcache_candle_batch *b, void *flush_ctx)
{
	(void)c; (void)b; (void)flush_ctx;
	return NULL;
}

/**
 * @brief   Parses a duration string (e.g., "1h", "30m", "10s") into seconds.
 */
int parse_duration(const char* str)
{
	int val = atoi(str);
	char unit = str[strlen(str) - 1];
	if (unit == 'h' || unit == 'H') return val * 3600;
	if (unit == 'm' || unit == 'M') return val * 60;
	return val;
}


/**
 * @brief   Returns a string representation of a scenario_type.
 */
const char* scenario_to_string(scenario_type s)
{
	switch(s) {
		case SCENARIO_HFT: return "HFT";
		case SCENARIO_FLASH_CRASH: return "FlashCrash";
		case SCENARIO_GAPPING: return "Gapping";
		default: return "Unknown";
	}
}

/**
 * @brief   Returns a string representation of a candle_mode_type.
 */
const char* candle_mode_to_string(candle_mode_type m)
{
	switch(m) {
		case MODE_TIME_ONLY: return "Time-based Only";
		case MODE_TICK_ONLY: return "Tick-based Only";
		case MODE_BOTH: return "Time and Tick";
		default: return "Unknown";
	}
}
