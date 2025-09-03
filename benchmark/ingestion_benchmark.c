/**
 * @file   ingestion_benchmark.c
 * @brief  A benchmark measuring the data ingestion latency for the trcache
 * library. It measures the time from when a trade is created until it becomes
 * visible in the latest queryable candle.
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
 * @brief Benchmark configuration parameters, set via command-line arguments.
 */
typedef struct {
	int num_worker_threads;
	int num_feeder_threads;
	int num_querier_threads;
	int num_symbols;
	int duration_sec;
	int warmup_sec;
	int start_cpu_core;
	scenario_type scenario;
	candle_mode_type candle_mode;
	char* output_file_path;
} benchmark_config;

/**
 * @brief A snapshot of performance metrics for a single time interval.
 */
typedef struct {
	int timestamp;
	double throughput;
	double p99_latency_us;
	long discarded_samples;
	trcache_worker_distribution_stats worker_stats;
	trcache_memory_stats mem_stats;
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
	int *symbol_ids;
	_Atomic uint64_t* latest_creation_ns_per_symbol;
	_Atomic uint64_t* trade_id_per_symbol;
	_Atomic long discarded_sample_count;
	struct hdr_histogram* overall_latency_hist;
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

/**
 * @brief Arguments for each individual querier thread.
 */
typedef struct {
	shared_context *s_ctx;
	int querier_id;
} querier_thread_args;

// --- Function Prototypes ---
void* dummy_sync_flush(trcache *c, trcache_candle_batch *b, void *flush_ctx);
void* feeder_thread_main(void *arg);
void* querier_thread_main(void *arg);
void* monitor_thread_main(void *arg);
void print_time_series_data(const benchmark_config *config,
	shared_context *ctx, FILE* out_file);
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
	if (argc < 11) {
		fprintf(
			stderr,
			"Usage: %s <workers> <feeders> <queriers> <symbols> <duration> <warmup> "
			"<scenario> <candle_mode> <start_core> <output_file>\n",
			argv[0]);
		fprintf(stderr, "  Duration format: e.g., 1h, 30m, 10s\n");
		fprintf(stderr, "  Scenario: 0=HFT, 1=FlashCrash, 2=Gapping\n");
		fprintf(
			stderr, "  Candle Mode: 0=TimeOnly, 1=TickOnly, 2=Both\n");
		return 1;
	}

	benchmark_config config = {
		.num_worker_threads = atoi(argv[1]),
		.num_feeder_threads = atoi(argv[2]),
		.num_querier_threads = atoi(argv[3]),
		.num_symbols = atoi(argv[4]),
		.duration_sec = parse_duration(argv[5]),
		.warmup_sec = parse_duration(argv[6]),
		.scenario = (scenario_type)atoi(argv[7]),
		.candle_mode = (candle_mode_type)atoi(argv[8]),
		.start_cpu_core = atoi(argv[9]),
		.output_file_path = argv[10],
	};

	if (config.duration_sec <= 0 || config.warmup_sec < 0) {
		return 1;
	}

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
	ctx.latest_creation_ns_per_symbol
		= calloc(config.num_symbols, sizeof(_Atomic uint64_t));
	ctx.trade_id_per_symbol
		= calloc(config.num_symbols, sizeof(_Atomic uint64_t));
	for (int i = 0; i < config.num_symbols; ++i) {
		atomic_init(&ctx.trade_id_per_symbol[i], 0);
	}
	hdr_histogram_init(1, 1000000000, 3, &ctx.overall_latency_hist);
	ctx.time_series_stats =
		calloc(config.duration_sec, sizeof(periodic_stats));

	// --- Thread Creation & Execution ---
	pthread_t feeders[config.num_feeder_threads];
	feeder_thread_args f_args[config.num_feeder_threads];
	pthread_t queriers[config.num_querier_threads];
	querier_thread_args q_args[config.num_querier_threads];
	pthread_t monitor;
	printf("Starting benchmark...\n");
	for (int i = 0; i < config.num_feeder_threads; ++i) {
		f_args[i] = (feeder_thread_args){ .s_ctx = &ctx, .feeder_id = i };
		pthread_create(&feeders[i], NULL, feeder_thread_main, &f_args[i]);
	}
	for (int i = 0; i < config.num_querier_threads; ++i) {
		q_args[i] = (querier_thread_args){ .s_ctx = &ctx, .querier_id = i };
		pthread_create(&queriers[i], NULL, querier_thread_main, &q_args[i]);
	}
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
	for (int i = 0; i < config.num_querier_threads; ++i) {
		pthread_join(queriers[i], NULL);
	}
	pthread_join(monitor, NULL);

	FILE* out_file = fopen(config.output_file_path, "w");
	if (!out_file) {
		perror("Failed to open output file");
		return 1;
	}

	print_time_series_data(&config, &ctx, out_file);

	fclose(out_file);

	// --- Cleanup ---
	trcache_destroy(cache);
	free(symbol_ids);
    free(ctx.latest_creation_ns_per_symbol);
	free(ctx.trade_id_per_symbol);
	hdr_histogram_close(ctx.overall_latency_hist);
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

	time_t start_time = time(NULL);

	int num_feeders = config->num_feeder_threads;
	int symbols_per_feeder = config->num_symbols / num_feeders;
	int start_idx = f_args->feeder_id * symbols_per_feeder;
	int end_idx = start_idx + symbols_per_feeder;
	if (f_args->feeder_id == num_feeders - 1) {
		end_idx = config->num_symbols;
	}

	while (!atomic_load(&ctx->should_stop)) {
		time_t elapsed_sec = time(NULL) - start_time;
		if (config->scenario == SCENARIO_FLASH_CRASH) {
			if ((elapsed_sec % 60) > 5) usleep(1000);
		} else if (config->scenario == SCENARIO_GAPPING) {
			if (rand() % 100 < 5) {
				usleep(50000 + (rand() % 50000));
			}
		}

		for (int i = start_idx; i < end_idx; ++i) {
			struct timespec now;
			clock_gettime(CLOCK_MONOTONIC, &now);
			uint64_t now_ns = (uint64_t)now.tv_sec * 1000000000 + now.tv_nsec;
			uint64_t trade_id = ctx->trade_id_per_symbol[i]++;

			struct trcache_trade_data td = {
				.timestamp = now_ns / 1000000,
                .trade_id = trade_id,
				.price = (double)now_ns,
                .volume = (rand() % 10 == 0) ?
					(double)(rand() % 1000 + 100) : (double)(rand() % 100 + 1),
			};

			trcache_feed_trade_data(ctx->cache, &td, ctx->symbol_ids[i]);

			atomic_store_explicit(&ctx->latest_creation_ns_per_symbol[i],
				now_ns, memory_order_release);

			if (atomic_load(&ctx->warmup_done)) {
				atomic_fetch_add(&ctx->total_trades_fed, 1);
			}
		}
	}
	return NULL;
}

/**
 * @brief   Querier thread that measures end-to-end data propagation latency.
 */
void* querier_thread_main(void *arg)
{
	querier_thread_args *q_args = (querier_thread_args *)arg;
	shared_context *ctx = q_args->s_ctx;
	const benchmark_config *config = ctx->config;
	int core_id = config->start_cpu_core;

	if (core_id >= 0) {
		set_thread_affinity(core_id + config->num_feeder_threads);
	}

	TRCACHE_DEFINE_BATCH_ON_STACK(batch, 1, TRCACHE_CLOSE);
	trcache_candle_type any_candle_type = { .base = 0, .type_idx = 0 };
    if (config->candle_mode == MODE_TICK_ONLY) {
        any_candle_type.base = CANDLE_TICK_BASE;
    } else {
        any_candle_type.base = CANDLE_TIME_BASE;
    }

	while (!atomic_load(&ctx->should_stop)) {
		int symbol_idx = rand() % config->num_symbols;
		int target_symbol_id = ctx->symbol_ids[symbol_idx];
		uint64_t latest_ts_marker = atomic_load_explicit(
				&ctx->latest_creation_ns_per_symbol[symbol_idx],
				memory_order_acquire);

		if (latest_ts_marker == 0) {
			usleep(100);
			continue;
		}

		struct timespec start_time, end_time;
		clock_gettime(CLOCK_MONOTONIC, &start_time);

		while(true) {
			int ret = trcache_get_candles_by_symbol_id_and_offset(
				ctx->cache, target_symbol_id, any_candle_type,
				TRCACHE_CLOSE, 0, 1, &batch);

			if (ret == 0 && batch.num_candles > 0) {
                /*
				 * The close price holds the timestamp marker of the last trade
				 * in the candle.
				 */
				if (batch.close_array[0] >= (double)latest_ts_marker) {
					break; // The latest trade is now visible.
				}
			}
		}

		clock_gettime(CLOCK_MONOTONIC, &end_time);

		if (atomic_load(&ctx->warmup_done)) {
			uint64_t latency_ns
				= (uint64_t)(end_time.tv_sec - start_time.tv_sec) * 1000000000
					+ (end_time.tv_nsec - start_time.tv_nsec);
			pthread_mutex_lock(&ctx->hist_mutex);
			hdr_histogram_record_value(ctx->overall_latency_hist, latency_ns);
			pthread_mutex_unlock(&ctx->hist_mutex);
		}
		usleep(500); // Wait a bit before the next measurement
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
		stats->p99_latency_us = (double)hdr_histogram_value_at_percentile(
			ctx->overall_latency_hist, 99.0) / 1000.0;
		stats->discarded_samples =
			atomic_exchange(&ctx->discarded_sample_count, 0);
		hdr_histogram_reset(ctx->overall_latency_hist);
		pthread_mutex_unlock(&ctx->hist_mutex);

		trcache_get_worker_distribution(ctx->cache, &stats->worker_stats);
		trcache_get_total_memory_breakdown(ctx->cache, &stats->mem_stats);

		double mem_tdb_gb
			= (double)stats->mem_stats.usage_bytes[MEMSTAT_TRADE_DATA_BUFFER]
				/ (1024*1024*1024);
		double mem_ccl_gb
			= (double)stats->mem_stats.usage_bytes[MEMSTAT_CANDLE_CHUNK_LIST]
				/ (1024*1024*1024);
		double mem_cci_gb
			= (double)stats->mem_stats.usage_bytes[MEMSTAT_CANDLE_CHUNK_INDEX]
				/ (1024*1024*1024);
		double mem_scq_gb
			= (double)stats->mem_stats.usage_bytes[MEMSTAT_SCQ_NODE]
				/ (1024*1024*1024);
		double mem_msg_gb
			= (double)stats->mem_stats.usage_bytes[MEMSTAT_SCHED_MSG]
				/ (1024*1024*1024);

		printf("\r[Time: %3ds] Throughput: %8.0f trades/s | p99 Latency (us): %7.3f | Workers (A/C/F): %d/%d/%d | Mem (GB) - TDB:%.2f, CCL:%.2f, CCI:%.2f, SCQ:%.2f, MSG:%.2f",
			stats->timestamp,
			stats->throughput,
			stats->p99_latency_us,
			stats->worker_stats.stage_limits[WORKER_STAT_STAGE_APPLY],
			stats->worker_stats.stage_limits[WORKER_STAT_STAGE_CONVERT],
			stats->worker_stats.stage_limits[WORKER_STAT_STAGE_FLUSH],
			mem_tdb_gb, mem_ccl_gb, mem_cci_gb, mem_scq_gb, mem_msg_gb);
		fflush(stdout);
	}
	printf("\n");
	return NULL;
}

/**
 * @brief   Prints the collected time-series data in a CSV-friendly format.
 */
void print_time_series_data(const benchmark_config *config, shared_context *ctx,
	FILE* out_file)
{
	fprintf(out_file, "\n--- Time-Series Performance Data (1-second intervals) ---\n");
	fprintf(out_file,
		"%-8s %-15s %-15s %-10s %-10s %-10s %-10s %-15s %-15s %-15s %-15s %-15s\n",
		"Time(s)", "Throughput", "p99_Lat(us)", "Discarded", "Apply", "Convert", "Flush",
		"TDBuf(Bytes)", "CCList(Bytes)", "CCIdx(Bytes)", "SCQ(Bytes)", "SchedMsg(Bytes)");
	fprintf(out_file,
		"-----------------------------------------------------------------"
		"-----------------------------------------------------------------"
		"----------------------------------------------------\n");
	for(int i = 0; i < config->duration_sec; ++i) {
		periodic_stats* s = &ctx->time_series_stats[i];
		if (s->timestamp == 0) continue;

		fprintf(out_file,
			"%-8d %-15.0f %-15.3f %-10ld %-10d %-10d %-10d %-15zu %-15zu %-15zu %-15zu %-15zu\n",
			s->timestamp, s->throughput, s->p99_latency_us, s->discarded_samples,
			s->worker_stats.stage_limits[WORKER_STAT_STAGE_APPLY],
			s->worker_stats.stage_limits[WORKER_STAT_STAGE_CONVERT],
			s->worker_stats.stage_limits[WORKER_STAT_STAGE_FLUSH],
			s->mem_stats.usage_bytes[MEMSTAT_TRADE_DATA_BUFFER],
			s->mem_stats.usage_bytes[MEMSTAT_CANDLE_CHUNK_LIST],
			s->mem_stats.usage_bytes[MEMSTAT_CANDLE_CHUNK_INDEX],
			s->mem_stats.usage_bytes[MEMSTAT_SCQ_NODE],
			s->mem_stats.usage_bytes[MEMSTAT_SCHED_MSG]);
	}
	fprintf(out_file,
		"-----------------------------------------------------------------"
		"-----------------------------------------------------------------"
		"----------------------------------------------------\n");
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
