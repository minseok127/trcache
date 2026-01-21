/**
 * @file benchmark/static_read_benchmark.c
 * @brief Static read (OLAP) benchmark for trcache.
 *
 * This benchmark measures read performance in a static scenario where:
 * 1. All trades have been fed and converted to column batches
 * 2. No concurrent writes are occurring
 * 3. Multiple reader threads query candle data concurrently
 *
 * Measures latency distribution across various query sizes, field selections,
 * and concurrency levels. Compares SIMD-enabled vs scalar builds.
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
#include <assert.h>

#include "trcache.h"
#include "hdr_histogram.h"

/* Error logging macro */
#define errmsg(stream, fmt, ...)                         \
	do {                                                 \
		fprintf((stream), "[%s:%d:%s] " fmt,             \
		__FILE__, __LINE__, __func__, ##__VA_ARGS__);    \
	} while (0)

/* Constants */
#define NUM_SYMBOLS (1)
#define NUM_CANDLE_TYPES (2)
#define WARMUP_TRADES_PER_SYMBOL (1000000)
#define QUERIES_PER_CONFIG (10000)
#define ONE_MINUTE_MS (60000)

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

/* Field Definitions */
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
	int num_reader_threads;
	int num_worker_threads;
	const char *output_csv_path;
	int query_sizes[8];
	int num_query_sizes;
	bool key_based;
};

/* Test Configuration */
struct test_config {
	int query_size;
	int num_fields;
	const int *field_indices;
	bool key_based;
};

/* Reader Thread Result */
struct reader_result {
	struct hdr_histogram *latency_hist;
	uint64_t total_queries;
	uint64_t failed_queries;
};

/* Reader Thread Arguments */
struct reader_thread_args {
	int thread_idx;
	struct test_config test_cfg;
	struct reader_result *result;
};

/* Key tracking for key-based queries */
struct symbol_keys {
	uint64_t *keys;
	int count;
	int capacity;
};

/* Global State */
static struct trcache *g_cache = NULL;
static struct benchmark_config g_config;
static int g_symbol_ids[NUM_SYMBOLS];
static struct symbol_keys g_symbol_keys[NUM_SYMBOLS]; /* Track valid keys per symbol */
static _Atomic(bool) g_readers_start = false;
static _Atomic(bool) g_readers_stop = false;
static _Atomic(int) g_readers_ready = 0;

/* Candle update operations */
static void candle_init_tick(struct trcache_candle_base *c,
	void *data)
{
	struct trcache_trade_data *d = (struct trcache_trade_data *)data;
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
	void *data) \
{ \
	struct trcache_trade_data *d = (struct trcache_trade_data *)data; \
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
	} \
	return true; \
}

DEFINE_TICK_UPDATE_FUNC(3)

static void candle_init_time(struct trcache_candle_base *c,
	void *data)
{
	struct trcache_trade_data *d = (struct trcache_trade_data *)data;
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
	void *data) \
{ \
	struct trcache_trade_data *d = (struct trcache_trade_data *)data; \
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

/**
 * @brief   Warmup phase: feed trades to populate candles and track keys.
 */
static int warmup_phase(void)
{
	struct trcache_trade_data trade;
	unsigned int rand_state = (unsigned int)time(NULL);
	int candle_idx = 0; /* Use 3-tick candle for key tracking */

	printf("==> Warmup: Feeding %d trades per symbol...\n",
		WARMUP_TRADES_PER_SYMBOL);

	/* Initialize key tracking arrays */
	for (int sym_idx = 0; sym_idx < NUM_SYMBOLS; sym_idx++) {
		g_symbol_keys[sym_idx].capacity = 10000;
		g_symbol_keys[sym_idx].keys = malloc(
			g_symbol_keys[sym_idx].capacity * sizeof(uint64_t));
		g_symbol_keys[sym_idx].count = 0;
		
		if (g_symbol_keys[sym_idx].keys == NULL) {
			errmsg(stderr, "Failed to allocate key tracking array\n");
			return -1;
		}
	}

	/* Feed trades with unique trade_id per symbol */
	for (int sym_idx = 0; sym_idx < NUM_SYMBOLS; sym_idx++) {
		int symbol_id = g_symbol_ids[sym_idx];
		uint64_t base_ts = 1609459200000ULL; /* 2021-01-01 00:00:00 UTC */
		uint64_t trade_id = (uint64_t)sym_idx * 1000000; /* Unique range per symbol */

		for (int i = 0; i < WARMUP_TRADES_PER_SYMBOL; i++) {
			trade.timestamp = base_ts + (i / 10) * 1000; /* ~10 trades/sec */
			trade.trade_id = trade_id++;
			trade.price.as_double = 100.0 +
				((double)rand_r(&rand_state) / RAND_MAX * 10.0);
			trade.volume.as_double = 1.0 +
				((double)rand_r(&rand_state) / RAND_MAX * 100.0);

			while (trcache_feed_trade_data(g_cache, &trade, symbol_id) != 0) {
				sched_yield();
			}
		}

		if ((sym_idx + 1) % 100 == 0) {
			printf("  Fed %d/%d symbols\n", sym_idx + 1, NUM_SYMBOLS);
		}
	}

	printf("==> Warmup: Waiting for conversion to complete...\n");
	sleep(3); /* Allow pipeline to drain */

	/* Now collect actual candle keys for each symbol */
	printf("==> Warmup: Collecting candle keys...\n");
	int field_indices[] = {0}; /* Just open field for key collection */
	struct trcache_field_request request = {
		.field_indices = field_indices,
		.num_fields = 1
	};

	for (int sym_idx = 0; sym_idx < NUM_SYMBOLS; sym_idx++) {
		int symbol_id = g_symbol_ids[sym_idx];
		
		/* Query up to 10000 candles to get their keys */
		struct trcache_candle_batch *batch = trcache_batch_alloc_on_heap(
			g_cache, candle_idx, 10000, &request);
		
		if (batch == NULL) {
			errmsg(stderr, "Failed to allocate batch for key collection\n");
			return -1;
		}

		int ret = trcache_get_candles_by_symbol_id_and_offset(
			g_cache, symbol_id, candle_idx, &request, 0, 10000, batch);

		if (ret == 0 && batch->num_candles > 0) {
			g_symbol_keys[sym_idx].count = batch->num_candles;
			for (int i = 0; i < batch->num_candles; i++) {
				g_symbol_keys[sym_idx].keys[i] = batch->key_array[i];
			}
		}

		trcache_batch_free(batch);

		if ((sym_idx + 1) % 100 == 0) {
			printf("  Collected keys for %d/%d symbols\n", sym_idx + 1, NUM_SYMBOLS);
		}
	}

	printf("==> Warmup complete.\n");
	return 0;
}

/**
 * @brief   Reader thread main function.
 */
static void* reader_thread_main(void *arg)
{
	struct reader_thread_args *args = (struct reader_thread_args *)arg;
	struct test_config *cfg = &args->test_cfg;
	struct reader_result *result = args->result;
	unsigned int rand_state = (unsigned int)(time(NULL) ^ pthread_self());
	struct trcache_field_request request = {
		.field_indices = cfg->field_indices,
		.num_fields = cfg->num_fields
	};
	struct trcache_candle_batch *batch = NULL;
	int candle_idx = 0; /* Use 3-tick candle for queries */
	struct timespec start_ts, end_ts;
	uint64_t latency_ns;

	/* Allocate batch for queries */
	batch = trcache_batch_alloc_on_heap(g_cache, candle_idx,
		cfg->query_size, &request);
	if (batch == NULL) {
		errmsg(stderr, "Failed to allocate batch\n");
		return NULL;
	}

	/* Signal ready */
	atomic_fetch_add(&g_readers_ready, 1);

	/* Wait for start signal */
	while (!atomic_load(&g_readers_start)) {
		__asm__ __volatile__("pause");
	}

	/* Main query loop */
	while (!atomic_load(&g_readers_stop)) {
		int sym_idx = rand_r(&rand_state) % NUM_SYMBOLS;
		int symbol_id = g_symbol_ids[sym_idx];
		int ret;

		clock_gettime(CLOCK_MONOTONIC, &start_ts);

		if (!cfg->key_based) {
			/* Offset-based sequential access */
			int offset = rand_r(&rand_state) % 10000;
			ret = trcache_get_candles_by_symbol_id_and_offset(
				g_cache, symbol_id, candle_idx, &request,
				offset, cfg->query_size, batch);
		} else {
			/* Key-based sequential access - use actual existing keys */
			if (g_symbol_keys[sym_idx].count == 0) {
				/* No keys available for this symbol, skip */
				continue;
			}
			
			/* Pick a random valid key */
			int key_idx = rand_r(&rand_state) % g_symbol_keys[sym_idx].count;
			uint64_t target_key = g_symbol_keys[sym_idx].keys[key_idx];
			
			ret = trcache_get_candles_by_symbol_id_and_key(
				g_cache, symbol_id, candle_idx, &request,
				target_key, cfg->query_size, batch);
		}

		clock_gettime(CLOCK_MONOTONIC, &end_ts);
		
		latency_ns = (end_ts.tv_sec - start_ts.tv_sec) * 1000000000ULL +
			(end_ts.tv_nsec - start_ts.tv_nsec);

		if (ret == 0) {
			hdr_histogram_record_value(result->latency_hist,
				(int64_t)(latency_ns));
			result->total_queries++;
		} else {
			result->failed_queries++;
		}
	}

	trcache_batch_free(batch);
	return NULL;
}

/**
 * @brief   Run a single test configuration.
 */
static int run_test(struct test_config *cfg, FILE *csv_file)
{
	pthread_t *threads = NULL;
	struct reader_thread_args *args = NULL;
	struct reader_result *results = NULL;
	struct hdr_histogram *merged_hist = NULL;
	int ret = -1;

	printf("\n==> Running test: size=%d, fields=%d, %s\n",
		cfg->query_size, cfg->num_fields,
		cfg->key_based ? "key-based" : "offset-based");

	/* Allocate resources */
	threads = calloc(g_config.num_reader_threads, sizeof(pthread_t));
	args = calloc(g_config.num_reader_threads,
		sizeof(struct reader_thread_args));
	results = calloc(g_config.num_reader_threads,
		sizeof(struct reader_result));

	if (!threads || !args || !results) {
		errmsg(stderr, "Allocation failed\n");
		goto cleanup;
	}

	/* Initialize per-thread results */
	for (int i = 0; i < g_config.num_reader_threads; i++) {
		if (hdr_histogram_init(1, 10000000, 3, &results[i].latency_hist) != 0) {
			errmsg(stderr, "HDR histogram init failed\n");
			goto cleanup;
		}
		results[i].total_queries = 0;
		results[i].failed_queries = 0;

		args[i].thread_idx = i;
		args[i].test_cfg = *cfg;
		args[i].result = &results[i];
	}

	/* Reset synchronization flags */
	atomic_store(&g_readers_ready, 0);
	atomic_store(&g_readers_start, false);
	atomic_store(&g_readers_stop, false);

	/* Create reader threads */
	for (int i = 0; i < g_config.num_reader_threads; i++) {
		if (pthread_create(&threads[i], NULL, reader_thread_main,
				&args[i]) != 0) {
			errmsg(stderr, "Failed to create reader thread %d\n", i);
			goto cleanup;
		}
	}

	/* Wait for all readers ready */
	while (atomic_load(&g_readers_ready) < g_config.num_reader_threads) {
		usleep(1000);
	}

	printf("  All readers ready. Starting queries...\n");

	/* Start queries */
	atomic_store(&g_readers_start, true);

	/* Wait until enough queries collected */
	uint64_t total = 0;
	while (total < QUERIES_PER_CONFIG) {
		usleep(100000); /* 100ms */
		total = 0;
		for (int i = 0; i < g_config.num_reader_threads; i++) {
			total += results[i].total_queries;
		}
	}

	/* Stop readers */
	atomic_store(&g_readers_stop, true);

	/* Join threads */
	for (int i = 0; i < g_config.num_reader_threads; i++) {
		pthread_join(threads[i], NULL);
	}

	printf("  Queries complete. Merging histograms...\n");

	/* Merge histograms */
	if (hdr_histogram_init(1, 10000000, 3, &merged_hist) != 0) {
		errmsg(stderr, "Failed to create merged histogram\n");
		goto cleanup;
	}

	total = 0;
	uint64_t failed = 0;
	for (int i = 0; i < g_config.num_reader_threads; i++) {
		/* Manually merge (simplified) */
		for (int j = 0; j < results[i].latency_hist->counts_len; j++) {
			merged_hist->counts[j] += results[i].latency_hist->counts[j];
		}
		merged_hist->total_count += results[i].latency_hist->total_count;
		total += results[i].total_queries;
		failed += results[i].failed_queries;
	}

	/* Write to CSV */
	fprintf(csv_file, "%d,%d,%s,%d,%lu,%lu,%.2f,%.2f,%.2f,%.2f,%.2f\n",
		cfg->query_size,
		cfg->num_fields,
		cfg->key_based ? "key-based" : "offset-based",
		g_config.num_reader_threads,
		total,
		failed,
		hdr_histogram_mean(merged_hist),
		(double)hdr_histogram_value_at_percentile(merged_hist, 50.0),
		(double)hdr_histogram_value_at_percentile(merged_hist, 95.0),
		(double)hdr_histogram_value_at_percentile(merged_hist, 99.0),
		(double)hdr_histogram_max(merged_hist));

	fflush(csv_file);

	printf("  Results: mean=%.2f ns, p50=%.2f, p99=%.2f, max=%.2f\n",
		hdr_histogram_mean(merged_hist),
		(double)hdr_histogram_value_at_percentile(merged_hist, 50.0),
		(double)hdr_histogram_value_at_percentile(merged_hist, 99.0),
		(double)hdr_histogram_max(merged_hist));

	ret = 0;

cleanup:
	if (merged_hist) hdr_histogram_close(merged_hist);
	if (results) {
		for (int i = 0; i < g_config.num_reader_threads; i++) {
			if (results[i].latency_hist) {
				hdr_histogram_close(results[i].latency_hist);
			}
		}
		free(results);
	}
	free(args);
	free(threads);

	return ret;
}

/**
 * @brief   Cleanup key tracking arrays.
 */
static void cleanup_key_tracking(void)
{
	for (int i = 0; i < NUM_SYMBOLS; i++) {
		free(g_symbol_keys[i].keys);
		g_symbol_keys[i].keys = NULL;
		g_symbol_keys[i].count = 0;
	}
}

static void print_usage(const char *prog_name)
{
	fprintf(stderr,
		"Usage: %s [options]\n\n"
		"Required Options:\n"
		"  -r, --reader-threads <N>   Number of concurrent reader threads\n"
		"  -w, --worker-threads <M>   Number of trcache internal workers\n"
		"  -o, --output-csv <path>    Path to output CSV file\n"
		"\n"
		"Optional Options:\n"
		"  -k, --key-based            Use key-based sequential access pattern (default: offset)\n"
		"  -h, --help                 Print this help message\n",
		prog_name);
}

static int parse_arguments(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	g_config.key_based = false;
	
	/* Default query sizes */
	g_config.query_sizes[0] = 10;
	g_config.query_sizes[1] = 100;
	g_config.query_sizes[2] = 1000;
	g_config.query_sizes[3] = 10000;
	g_config.query_sizes[4] = 100000;
	g_config.num_query_sizes = 5;

	const struct option long_options[] = {
		{"reader-threads", required_argument, 0, 'r'},
		{"worker-threads", required_argument, 0, 'w'},
		{"output-csv", required_argument, 0, 'o'},
		{"key-based", no_argument, 0, 'k'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "r:w:o:k:h", long_options, NULL)) != -1) {
		switch (c) {
			case 'r': g_config.num_reader_threads = atoi(optarg); break;
			case 'w': g_config.num_worker_threads = atoi(optarg); break;
			case 'o': g_config.output_csv_path = optarg; break;
			case 'k': g_config.key_based = true; break;
			case 'h': print_usage(argv[0]); return -1;
			case '?': default: return -1;
		}
	}

	if (g_config.num_reader_threads <= 0 || g_config.num_worker_threads <= 0 ||
		!g_config.output_csv_path) {
		errmsg(stderr, "Missing required arguments\n\n");
		print_usage(argv[0]);
		return -1;
	}

	return 0;
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
		.batch_candle_count_pow2 = 12,
		.cached_batch_count_pow2 = 5,
		.total_memory_limit = 5ULL * 1024 * 1024 * 1024,
		.num_worker_threads = g_config.num_worker_threads,
		.max_symbols = NUM_SYMBOLS,
		.trade_data_size = sizeof(struct trcache_trade_data)
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

int main(int argc, char **argv)
{
	FILE *csv_file = NULL;
	int ret_code = EXIT_SUCCESS;

	printf("trcache Static Read (OLAP) Benchmark\n");
	printf("======================================\n");

	if (parse_arguments(argc, argv) != 0) {
		return EXIT_FAILURE;
	}

	printf("Configuration:\n");
	printf("  Reader Threads: %d\n", g_config.num_reader_threads);
	printf("  Worker Threads: %d\n", g_config.num_worker_threads);
	printf("  Output CSV:     %s\n", g_config.output_csv_path);
	printf("  Access Pattern: %s\n",
		g_config.key_based ? "Key-based" : "Offset-based");
	printf("======================================\n");

	if (initialize_trcache() != 0) {
		errmsg(stderr, "Failed to initialize trcache\n");
		return EXIT_FAILURE;
	}

	if (warmup_phase() != 0) {
		errmsg(stderr, "Warmup phase failed\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	csv_file = fopen(g_config.output_csv_path, "w");
	if (csv_file == NULL) {
		errmsg(stderr, "Failed to open CSV file: %s\n",
			g_config.output_csv_path);
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	fprintf(csv_file,
		"QuerySize,NumFields,AccessPattern,ReaderThreads,"
		"TotalQueries,FailedQueries,MeanLatency_ns,P50_ns,P95_ns,P99_ns,Max_ns\n");
	fflush(csv_file);

	/* Test matrix: query sizes × field selections × access patterns */
	int all_fields[] = {0, 1, 2, 3, 4, 5};
	int subset_fields[] = {1, 2, 3}; /* high, low, close */

	for (int size_idx = 0; size_idx < g_config.num_query_sizes; size_idx++) {
		/* Test 1: All fields */
		struct test_config cfg1 = {
			.query_size = g_config.query_sizes[size_idx],
			.num_fields = 6,
			.field_indices = all_fields,
			.key_based = g_config.key_based
		};
		if (run_test(&cfg1, csv_file) != 0) {
			errmsg(stderr, "Test failed\n");
			ret_code = EXIT_FAILURE;
			goto cleanup;
		}

		/* Test 2: Subset fields */
		struct test_config cfg2 = {
			.query_size = g_config.query_sizes[size_idx],
			.num_fields = 3,
			.field_indices = subset_fields,
			.key_based = g_config.key_based
		};
		if (run_test(&cfg2, csv_file) != 0) {
			errmsg(stderr, "Test failed\n");
			ret_code = EXIT_FAILURE;
			goto cleanup;
		}
	}

	printf("\n======================================\n");
	printf("Benchmark complete. Results saved to: %s\n",
		g_config.output_csv_path);

cleanup:
	if (csv_file) fclose(csv_file);
	if (g_cache) trcache_destroy(g_cache);
	cleanup_key_tracking();

	return ret_code;
}
