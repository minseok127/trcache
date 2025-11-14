/**
 * @file benchmark/htap_benchmark.c
 * @brief HTAP (Hybrid Transactional/Analytical Processing) benchmark.
 *
 * Measures system behavior under concurrent OLTP (feed) and OLAP (read)
 * workloads. Tests throughput stability, latency degradation, and memory
 * pressure handling over time.
 *
 * Benchmark phases:
 * - Phase 1 (0-30s):   OLTP only (baseline)
 * - Phase 2 (30-90s):  OLTP + Light OLAP
 * - Phase 3 (90-150s): OLTP + Heavy OLAP
 * - Phase 4 (150-180s): OLTP only (recovery)
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include <getopt.h>
#include <time.h>
#include <sched.h>
#include <math.h>
#include <errno.h>

#include "trcache.h"
#include "hdr_histogram.h"

#define errmsg(stream, fmt, ...)                         \
	do {                                                 \
		fprintf((stream), "[%s:%d:%s] " fmt,             \
		__FILE__, __LINE__, __func__, ##__VA_ARGS__);    \
	} while (0)

/* Constants */
#define NUM_SYMBOLS (4096)
#define NUM_CANDLE_TYPES (2)
#define DEFAULT_ZIPF_S (0.99)
#define ONE_MINUTE_MS (60000)

/* Benchmark Phases */
enum benchmark_phase {
	PHASE_OLTP_ONLY_BASELINE = 0,
	PHASE_OLTP_LIGHT_OLAP = 1,
	PHASE_OLTP_HEAVY_OLAP = 2,
	PHASE_OLTP_ONLY_RECOVERY = 3,
	PHASE_DONE = 4
};

/* Phase Configuration */
struct phase_config {
	enum benchmark_phase phase;
	int duration_sec;
	int num_readers;
	const char *name;
};

static struct phase_config g_phases[] = {
	{PHASE_OLTP_ONLY_BASELINE, 10, 0, "OLTP-Only (Baseline)"},
	{PHASE_OLTP_LIGHT_OLAP, 10, 2, "OLTP + Light OLAP"},
	{PHASE_OLTP_HEAVY_OLAP, 10, 8, "OLTP + Heavy OLAP"},
	{PHASE_OLTP_ONLY_RECOVERY, 10, 0, "OLTP-Only (Recovery)"}
};

#define NUM_PHASES (sizeof(g_phases) / sizeof(g_phases[0]))

/* Candle Structure */
struct my_candle {
	struct trcache_candle_base base;
	double open;
	double high;
	double low;
	double close;
	double volume;
	double amount;
	uint32_t trade_count;
};

const struct trcache_field_def g_candle_fields[] = {
	{offsetof(struct my_candle, open), sizeof(double), FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, high), sizeof(double), FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, low), sizeof(double), FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, close), sizeof(double), FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, volume), sizeof(double), FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, amount), sizeof(double), FIELD_TYPE_DOUBLE},
};

/* Global Configuration */
struct benchmark_config {
	int num_feed_threads;
	int num_worker_threads;
	const char *output_csv_path;
	double zipf_s;
	int read_delay_us;
};

/* Per-thread padded counter */
struct padded_counter {
	uint64_t count;
	char padding[64 - sizeof(uint64_t)];
} __attribute__((aligned(64)));

/* Reader thread state */
struct reader_state {
	pthread_t thread;
	_Atomic bool active;
	struct hdr_histogram *latency_hist;
	struct padded_counter query_count;
	struct padded_counter failed_count;
	int thread_idx;
};

/* Global State */
static struct trcache *g_cache = NULL;
static struct benchmark_config g_config;
static _Atomic bool g_running = true;
static int g_symbol_ids[NUM_SYMBOLS];
static FILE *g_csv_file = NULL;
static pthread_mutex_t g_csv_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct padded_counter *g_feed_counters = NULL;
static struct reader_state *g_reader_states = NULL;
static int g_max_readers = 0;
static _Atomic int g_current_phase = PHASE_OLTP_ONLY_BASELINE;

/* Candle callbacks (same as static_read_benchmark) */
static void candle_init_tick(struct trcache_candle_base *c,
	struct trcache_trade_data *d)
{
	struct my_candle *candle = (struct my_candle *)c;
	double price = d->price.as_double;
	double volume = d->volume.as_double;
	c->key.trade_id = d->trade_id;
	c->is_closed = false;
	candle->open = price;
	candle->high = price;
	candle->low = price;
	candle->close = price;
	candle->volume = volume;
	candle->amount = price * volume;
	candle->trade_count = 1;
}

#define DEFINE_TICK_UPDATE_FUNC(N) \
static bool candle_update_tick_##N(struct trcache_candle_base *c, \
	struct trcache_trade_data *d) \
{ \
	struct my_candle *candle = (struct my_candle *)c; \
	double price = d->price.as_double; \
	double volume = d->volume.as_double; \
	if (price > candle->high) candle->high = price; \
	if (price < candle->low) candle->low = price; \
	candle->close = price; \
	candle->volume += volume; \
	candle->amount += price * volume; \
	candle->trade_count++; \
	if (candle->trade_count >= N) { \
		c->is_closed = true; \
		return false; \
	} \
	return true; \
}

DEFINE_TICK_UPDATE_FUNC(3)

static void candle_init_time(struct trcache_candle_base *c,
	struct trcache_trade_data *d)
{
	struct my_candle *candle = (struct my_candle *)c;
	double price = d->price.as_double;
	double volume = d->volume.as_double;
	c->key.timestamp = d->timestamp - (d->timestamp % ONE_MINUTE_MS);
	c->is_closed = false;
	candle->open = price;
	candle->high = price;
	candle->low = price;
	candle->close = price;
	candle->volume = volume;
	candle->amount = price * volume;
	candle->trade_count = 1;
}

#define DEFINE_TIME_UPDATE_FUNC(SUFFIX, DURATION_MS) \
static bool candle_update_time_##SUFFIX(struct trcache_candle_base *c, \
	struct trcache_trade_data *d) \
{ \
	if (d->timestamp >= c->key.timestamp + (DURATION_MS)) { \
		c->is_closed = true; \
		return false; \
	} \
	struct my_candle *candle = (struct my_candle *)c; \
	double price = d->price.as_double; \
	double volume = d->volume.as_double; \
	if (price > candle->high) candle->high = price; \
	if (price < candle->low) candle->low = price; \
	candle->close = price; \
	candle->volume += volume; \
	candle->amount += price * volume; \
	candle->trade_count++; \
	return true; \
}

DEFINE_TIME_UPDATE_FUNC(1min, ONE_MINUTE_MS)

static void* sync_flush_noop(struct trcache *cache,
	struct trcache_candle_batch *batch, void *ctx)
{
	(void)cache; (void)batch; (void)ctx;
	return NULL;
}

/* Zipf distribution helpers (simplified from feed_only_benchmark) */
static void init_partition_zipf_generator(int start_rank, int num_symbols,
	double s, double *cdf_array)
{
	if (num_symbols <= 0) return;
	if (s == 0.0) {
		for (int i = 0; i < num_symbols; i++) {
			cdf_array[i] = (double)(i + 1) / num_symbols;
		}
		return;
	}
	double partition_sum = 0.0;
	for (int i = 0; i < num_symbols; i++) {
		partition_sum += 1.0 / pow((double)(start_rank + i), s);
	}
	double cumulative = 0.0;
	if (partition_sum > 0) {
		for (int i = 0; i < num_symbols; i++) {
			cumulative += (1.0 / pow((double)(start_rank + i), s)) / partition_sum;
			cdf_array[i] = cumulative;
		}
	} else {
		for (int i = 0; i < num_symbols; i++) {
			cdf_array[i] = (double)(i + 1) / num_symbols;
		}
	}
	cdf_array[num_symbols - 1] = 1.0;
}

static int get_next_partition_symbol_idx(int num_symbols,
	const double *cdf_array, unsigned int *rand_state)
{
	if (num_symbols <= 0) return 0;
	double u = (double)rand_r(rand_state) / (double)RAND_MAX;
	int lo = 0, hi = num_symbols - 1;
	while (lo < hi) {
		int mid = lo + ((hi - lo) >> 1);
		if (cdf_array[mid] < u) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo;
}

static uint64_t get_total_feed_count(void)
{
	uint64_t total = 0;
	if (g_feed_counters == NULL) return 0;
	for (int i = 0; i < g_config.num_feed_threads; i++) {
		total += g_feed_counters[i].count;
	}
	return total;
}

/* Reader thread main */
static void* reader_thread_main(void *arg)
{
	struct reader_state *state = (struct reader_state *)arg;
	unsigned int rand_state = (unsigned int)(time(NULL) ^ pthread_self());
	int candle_idx = 0;
	int query_size = 100;
	int field_indices[] = {1, 2, 3}; /* high, low, close */
	struct trcache_field_request request = {
		.field_indices = field_indices,
		.num_fields = 3
	};
	struct trcache_candle_batch *batch = NULL;
	struct timespec start_ts, end_ts;
	struct timespec read_delay = {
		.tv_sec = g_config.read_delay_us / 1000000,
		.tv_nsec = (g_config.read_delay_us % 1000000) * 1000
	};
	bool has_delay = (read_delay.tv_sec > 0 || read_delay.tv_nsec > 0);

	batch = trcache_batch_alloc_on_heap(g_cache, candle_idx,
		query_size, &request);
	if (batch == NULL) {
		errmsg(stderr, "Reader %d: Failed to allocate batch\n",
			state->thread_idx);
		return NULL;
	}

	printf("  [Reader %d] started.\n", state->thread_idx);

	while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
		if (!atomic_load_explicit(&state->active, memory_order_acquire)) {
			usleep(10000);
			continue;
		}

		int symbol_id = g_symbol_ids[rand_r(&rand_state) % NUM_SYMBOLS];
		int offset = rand_r(&rand_state) % 500;
		
		clock_gettime(CLOCK_MONOTONIC, &start_ts);
		int ret = trcache_get_candles_by_symbol_id_and_offset(
			g_cache, symbol_id, candle_idx, &request,
			offset, query_size, batch);
		clock_gettime(CLOCK_MONOTONIC, &end_ts);

		if (ret == 0) {
			uint64_t latency_ns = (end_ts.tv_sec - start_ts.tv_sec) * 1000000000ULL +
				(end_ts.tv_nsec - start_ts.tv_nsec);
			hdr_histogram_record_value(state->latency_hist, (int64_t)latency_ns);
			state->query_count.count++;
		} else {
			state->failed_count.count++;
		}

		if (has_delay) {
			nanosleep(&read_delay, NULL);
		}
	}

	printf("  [Reader %d] stopping.\n", state->thread_idx);
	trcache_batch_free(batch);
	return NULL;
}

/* Feed thread (adapted from feed_only_benchmark) */
struct feed_thread_args {
	int thread_idx;
};

static void* feed_thread_main(void *arg)
{
	struct feed_thread_args *args = (struct feed_thread_args *)arg;
	struct padded_counter *my_counter = &g_feed_counters[args->thread_idx];
	struct trcache_trade_data trade;
	unsigned int rand_state = (unsigned int)(time(NULL) ^ pthread_self());
	uint64_t trade_id = 0;

	printf("  [Feed Thread %d] started.\n", args->thread_idx);

	int num_feed_threads = g_config.num_feed_threads;
	int partition_size = NUM_SYMBOLS / num_feed_threads;
	int start_idx = args->thread_idx * partition_size;
	int end_idx = (args->thread_idx == num_feed_threads - 1) ?
		NUM_SYMBOLS : (args->thread_idx + 1) * partition_size;
	int symbols_in_partition = end_idx - start_idx;

	double *local_zipf_cdf = NULL;
	if (symbols_in_partition > 0) {
		local_zipf_cdf = malloc(symbols_in_partition * sizeof(double));
		if (local_zipf_cdf == NULL) {
			errmsg(stderr, "Feed thread %d: CDF alloc failed\n",
				args->thread_idx);
			return NULL;
		}
		init_partition_zipf_generator(start_idx + 1, symbols_in_partition,
			g_config.zipf_s, local_zipf_cdf);
	}

	while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
		if (symbols_in_partition <= 0) {
			sched_yield();
			continue;
		}

		int relative_idx = get_next_partition_symbol_idx(symbols_in_partition,
			local_zipf_cdf, &rand_state);
		int symbol_idx = start_idx + relative_idx;
		int symbol_id = g_symbol_ids[symbol_idx];

		trade_id++;
		trade.timestamp = (uint64_t)time(NULL) * 1000;
		trade.trade_id = trade_id;
		trade.price.as_double = 100.0 +
			((double)rand_r(&rand_state) / RAND_MAX);
		trade.volume.as_double = 1.0 +
			((double)rand_r(&rand_state) / RAND_MAX * 100.0);

		while (trcache_feed_trade_data(g_cache, &trade, symbol_id) != 0) {
			if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
				goto feed_loop_exit;
			}
			sched_yield();
		}

		my_counter->count++;
	}

feed_loop_exit:
	printf("  [Feed Thread %d] stopping.\n", args->thread_idx);
	free(local_zipf_cdf);
	return NULL;
}

static void write_csv_header(void)
{
	fprintf(g_csv_file,
		"Timestamp,ElapsedSec,Phase,PhaseName,"
		"FeedThreads,ActiveReaders,WorkerThreads,ZipfS,ReadDelay_us,"
		"FeedCountPerSec,QueryCountPerSec,FailedQueriesPerSec,"
		"QueryLatency_Mean_ns,QueryLatency_P50_ns,QueryLatency_P99_ns\n");
	fflush(g_csv_file);
}

/* Monitor and phase controller thread */
static void* monitor_thread_main(void *arg)
{
	(void)arg;
	time_t start_time = time(NULL);
	time_t last_log_time = start_time;
	uint64_t prev_feed_count = 0;
	uint64_t prev_query_count = 0;
	uint64_t prev_failed_count = 0;
	int current_phase_idx = 0;
	time_t phase_start_time = start_time;

	printf("  [Monitor Thread] started.\n");

	g_csv_file = fopen(g_config.output_csv_path, "w");
	if (g_csv_file == NULL) {
		errmsg(stderr, "Failed to open CSV file\n");
		atomic_store(&g_running, false);
		return NULL;
	}

	pthread_mutex_lock(&g_csv_mutex);
	write_csv_header();
	pthread_mutex_unlock(&g_csv_mutex);

	while (atomic_load_explicit(&g_running, memory_order_relaxed) &&
		current_phase_idx < (int)NUM_PHASES)
	{
		time_t now = time(NULL);
		if (now == last_log_time) {
			usleep(100000);
			continue;
		}
		last_log_time = now;

		int elapsed_sec = (int)(now - start_time);
		int phase_elapsed = (int)(now - phase_start_time);

		/* Phase transition check */
		if (phase_elapsed >= g_phases[current_phase_idx].duration_sec) {
			current_phase_idx++;
			if (current_phase_idx >= (int)NUM_PHASES) break;

			phase_start_time = now;
			printf("\n==> Entering Phase %d: %s\n",
				current_phase_idx + 1,
				g_phases[current_phase_idx].name);

			/* Adjust reader threads */
			int target_readers = g_phases[current_phase_idx].num_readers;
			for (int i = 0; i < g_max_readers; i++) {
				bool should_active = (i < target_readers);
				atomic_store_explicit(&g_reader_states[i].active,
					should_active, memory_order_release);
				if (should_active) {
					hdr_histogram_reset(g_reader_states[i].latency_hist);
				}
			}

			atomic_store(&g_current_phase,
				g_phases[current_phase_idx].phase);
		}

		/* Collect metrics */
		uint64_t curr_feed_count = get_total_feed_count();
		uint64_t feed_per_sec = curr_feed_count - prev_feed_count;

		uint64_t curr_query_count = 0;
		uint64_t curr_failed_count = 0;
		struct hdr_histogram *merged_hist = NULL;
		double mean_lat_ns = 0.0, p50_lat_ns = 0.0, p99_lat_ns = 0.0;

		int active_readers = g_phases[current_phase_idx].num_readers;
		if (active_readers > 0) {
			hdr_histogram_init(1, 10000000, 3, &merged_hist);
			for (int i = 0; i < active_readers; i++) {
				curr_query_count += g_reader_states[i].query_count.count;
				curr_failed_count += g_reader_states[i].failed_count.count;
				/* Merge histograms */
				for (int j = 0; j < g_reader_states[i].latency_hist->counts_len; j++) {
					merged_hist->counts[j] +=
						g_reader_states[i].latency_hist->counts[j];
				}
				merged_hist->total_count +=
					g_reader_states[i].latency_hist->total_count;
			}

			if (merged_hist && merged_hist->total_count > 0) {
				mean_lat_ns = hdr_histogram_mean(merged_hist);
				p50_lat_ns = (double)hdr_histogram_value_at_percentile(
					merged_hist, 50.0);
				p99_lat_ns = (double)hdr_histogram_value_at_percentile(
					merged_hist, 99.0);
			}
		}

		uint64_t query_per_sec = curr_query_count - prev_query_count;
		uint64_t failed_per_sec = curr_failed_count - prev_failed_count;

		pthread_mutex_lock(&g_csv_mutex);
		fprintf(g_csv_file, "%ld,%d,%d,\"%s\",%d,%d,%d,%.2f,%d,%lu,%lu,%lu",
			now, elapsed_sec,
			current_phase_idx, g_phases[current_phase_idx].name,
			g_config.num_feed_threads, active_readers,
			g_config.num_worker_threads, g_config.zipf_s,
			g_config.read_delay_us,
			feed_per_sec, query_per_sec, failed_per_sec);

		if (merged_hist && merged_hist->total_count > 0) {
			fprintf(g_csv_file, ",%.2f,%.2f,%.2f\n",
				mean_lat_ns, p50_lat_ns, p99_lat_ns);
		} else {
			fprintf(g_csv_file, ",0,0,0\n");
		}
		fflush(g_csv_file);
		pthread_mutex_unlock(&g_csv_mutex);

		printf("  [Monitor] %3d sec | Phase: %-22s | Feed/s: %-9lu "
			   "| Query/s: %-8lu | Lat(mean): %-7.0f ns "
			   "| Lat(p50): %-7.0f ns, | Lat(p99) %-7.0f ns\n",
			elapsed_sec,
			g_phases[current_phase_idx].name,
			feed_per_sec,
			query_per_sec,
			mean_lat_ns,
			p50_lat_ns,
			p99_lat_ns);

		if (merged_hist) hdr_histogram_close(merged_hist);

		prev_feed_count = curr_feed_count;
		prev_query_count = curr_query_count;
		prev_failed_count = curr_failed_count;
	}

	printf("  [Monitor Thread] stopping.\n");
	if (g_csv_file) {
		fclose(g_csv_file);
		g_csv_file = NULL;
	}
	return NULL;
}

static int initialize_trcache(void)
{
	const struct trcache_candle_update_ops g_update_ops[NUM_CANDLE_TYPES] = {
		[0] = { .init = candle_init_tick, .update = candle_update_tick_3 },
		[1] = { .init = candle_init_time, .update = candle_update_time_1min }
	};
	const struct trcache_batch_flush_ops g_flush_ops = {
		.flush = sync_flush_noop
	};
	const int num_fields = sizeof(g_candle_fields) /
		sizeof(struct trcache_field_def);
	const size_t candle_size = sizeof(struct my_candle);

	struct trcache_candle_config configs[NUM_CANDLE_TYPES] = {
		{
			.user_candle_size = candle_size,
			.field_definitions = g_candle_fields,
			.num_fields = num_fields,
			.update_ops = g_update_ops[0],
			.flush_ops = g_flush_ops,
		},
		{
			.user_candle_size = candle_size,
			.field_definitions = g_candle_fields,
			.num_fields = num_fields,
			.update_ops = g_update_ops[1],
			.flush_ops = g_flush_ops,
		}
	};

	struct trcache_init_ctx ctx = {
		.candle_configs = configs,
		.num_candle_configs = NUM_CANDLE_TYPES,
		.batch_candle_count_pow2 = 10,
		.cached_batch_count_pow2 = 0,
		.total_memory_limit = 5ULL * 1024 * 1024 * 1024,
		.num_worker_threads = g_config.num_worker_threads,
		.max_symbols = NUM_SYMBOLS
	};

	g_cache = trcache_init(&ctx);
	if (g_cache == NULL) {
		errmsg(stderr, "trcache_init() failed\n");
		return -1;
	}

	char symbol_name[16];
	for (int i = 0; i < NUM_SYMBOLS; i++) {
		snprintf(symbol_name, sizeof(symbol_name), "SYM%04d", i);
		g_symbol_ids[i] = trcache_register_symbol(g_cache, symbol_name);
		if (g_symbol_ids[i] < 0) {
			errmsg(stderr, "Failed to register symbol: %s\n", symbol_name);
			trcache_destroy(g_cache);
			g_cache = NULL;
			return -1;
		}
	}
	return 0;
}

static void print_usage(const char *prog_name)
{
	fprintf(stderr,
		"Usage: %s [options]\n\n"
		"Required Options:\n"
		"  -f, --feed-threads <N>     Number of feed threads\n"
		"  -w, --worker-threads <M>   Number of trcache workers\n"
		"  -o, --output-csv <path>    Path to output CSV\n"
		"\n"
		"Optional Options:\n"
		"  -s, --zipf-s <value>       Zipf exponent (default: %.2f)\n"
		"  -d, --read-delay <us>      Delay (think time) in μs between read queries\n"
		"                             (default: 0, i.e., max speed)\n"
		"  -h, --help                 Print this help\n",
		prog_name, DEFAULT_ZIPF_S);
}

static int parse_arguments(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	g_config.zipf_s = DEFAULT_ZIPF_S;

	const struct option long_options[] = {
		{"feed-threads", required_argument, 0, 'f'},
		{"worker-threads", required_argument, 0, 'w'},
		{"output-csv", required_argument, 0, 'o'},
		{"zipf-s", required_argument, 0, 's'},
		{"read-delay", required_argument, 0, 'd'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "f:w:o:s:d:h", long_options, NULL)) != -1) {
		switch (c) {
			case 'f': g_config.num_feed_threads = atoi(optarg); break;
			case 'w': g_config.num_worker_threads = atoi(optarg); break;
			case 'o': g_config.output_csv_path = optarg; break;
			case 's':
				g_config.zipf_s = atof(optarg);
				if (g_config.zipf_s < 0.0) {
					errmsg(stderr, "Zipf s must be >= 0\n");
					return -1;
				}
				break;
			case 'd':
				g_config.read_delay_us = atoi(optarg);
				if (g_config.read_delay_us < 0) {
					errmsg(stderr, "Read delay must be >= 0\n");
					return -1;
				}
				break;
			case 'h': print_usage(argv[0]); return -1;
			case '?': default: return -1;
		}
	}

	if (g_config.num_feed_threads <= 0 || g_config.num_worker_threads <= 0 ||
		!g_config.output_csv_path) {
		errmsg(stderr, "Missing required arguments\n\n");
		print_usage(argv[0]);
		return -1;
	}
	return 0;
}

int main(int argc, char **argv)
{
	pthread_t monitor_thread;
	pthread_t *feed_threads = NULL;
	struct feed_thread_args *feed_args = NULL;
	int ret_code = EXIT_SUCCESS;

	printf("trcache HTAP Benchmark\n");
	printf("======================\n");

	if (parse_arguments(argc, argv) != 0) {
		return EXIT_FAILURE;
	}

	/* Find max readers needed across all phases */
	g_max_readers = 0;
	for (size_t i = 0; i < NUM_PHASES; i++) {
		if (g_phases[i].num_readers > g_max_readers) {
			g_max_readers = g_phases[i].num_readers;
		}
	}

	g_feed_counters = calloc(g_config.num_feed_threads,
		sizeof(struct padded_counter));
	g_reader_states = calloc(g_max_readers, sizeof(struct reader_state));

	if (!g_feed_counters || !g_reader_states) {
		errmsg(stderr, "Memory allocation failed\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	printf("Configuration:\n");
	printf("  Feed Threads:   %d\n", g_config.num_feed_threads);
	printf("  Worker Threads: %d\n", g_config.num_worker_threads);
	printf("  Max Readers:    %d\n", g_max_readers);
	printf("  Output CSV:     %s\n", g_config.output_csv_path);
	printf("  Zipf Exponent:  %.2f\n", g_config.zipf_s);
	printf("  Read Delay (us): %d\n", g_config.read_delay_us);
	printf("======================\n");

	if (initialize_trcache() != 0) {
		errmsg(stderr, "trcache initialization failed\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	/* Initialize reader states */
	for (int i = 0; i < g_max_readers; i++) {
		g_reader_states[i].thread_idx = i;
		atomic_init(&g_reader_states[i].active, false);
		hdr_histogram_init(1, 10000000, 3, &g_reader_states[i].latency_hist);
		g_reader_states[i].query_count.count = 0;
		g_reader_states[i].failed_count.count = 0;
	}

	atomic_store(&g_running, true);

	/* Start reader threads */
	printf("Starting reader threads...\n");
	for (int i = 0; i < g_max_readers; i++) {
		if (pthread_create(&g_reader_states[i].thread, NULL,
				reader_thread_main, &g_reader_states[i]) != 0) {
			errmsg(stderr, "Failed to create reader %d\n", i);
			ret_code = EXIT_FAILURE;
			goto cleanup;
		}
	}

	/* Start feed threads */
	feed_threads = calloc(g_config.num_feed_threads, sizeof(pthread_t));
	feed_args = calloc(g_config.num_feed_threads,
		sizeof(struct feed_thread_args));
	if (!feed_threads || !feed_args) {
		errmsg(stderr, "Feed thread allocation failed\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	printf("Starting feed threads...\n");
	for (int i = 0; i < g_config.num_feed_threads; i++) {
		feed_args[i].thread_idx = i;
		if (pthread_create(&feed_threads[i], NULL,
				feed_thread_main, &feed_args[i]) != 0) {
			errmsg(stderr, "Failed to create feed thread %d\n", i);
			atomic_store(&g_running, false);
			for (int j = 0; j < i; j++) {
				pthread_join(feed_threads[j], NULL);
			}
			ret_code = EXIT_FAILURE;
			goto cleanup;
		}
	}

	/* Start monitor thread */
	printf("Starting monitor thread...\n");
	if (pthread_create(&monitor_thread, NULL, monitor_thread_main, NULL) != 0) {
		errmsg(stderr, "Failed to create monitor thread\n");
		atomic_store(&g_running, false);
		for (int i = 0; i < g_config.num_feed_threads; i++) {
			pthread_join(feed_threads[i], NULL);
		}
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	printf("\n==> Benchmark running through %zu phases...\n", NUM_PHASES);

	/* Wait for monitor to complete all phases */
	pthread_join(monitor_thread, NULL);

	/* Signal all threads to stop */
	printf("\n==> All phases complete. Stopping threads...\n");
	atomic_store(&g_running, false);

	/* Join feed threads */
	for (int i = 0; i < g_config.num_feed_threads; i++) {
		pthread_join(feed_threads[i], NULL);
	}
	printf("All feed threads joined.\n");

	/* Join reader threads */
	for (int i = 0; i < g_max_readers; i++) {
		pthread_join(g_reader_states[i].thread, NULL);
	}
	printf("All reader threads joined.\n");

cleanup:
	printf("Cleaning up resources...\n");

	if (g_cache) {
		printf("Destroying trcache...\n");
		trcache_destroy(g_cache);
		g_cache = NULL;
	}

	if (g_reader_states) {
		for (int i = 0; i < g_max_readers; i++) {
			if (g_reader_states[i].latency_hist) {
				hdr_histogram_close(g_reader_states[i].latency_hist);
			}
		}
		free(g_reader_states);
	}

	free(g_feed_counters);
	free(feed_threads);
	free(feed_args);

	if (g_csv_file) {
		fclose(g_csv_file);
		g_csv_file = NULL;
	}

	printf("Benchmark finished %s.\n",
		(ret_code == EXIT_SUCCESS) ? "successfully" : "with errors");
	if (ret_code == EXIT_SUCCESS && g_config.output_csv_path) {
		printf("Results saved to: %s\n", g_config.output_csv_path);
	}

	return ret_code;
}
