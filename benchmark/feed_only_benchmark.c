/**
 * @file benchmark/feed_only_benchmark.c
 * @brief Write-heavy (feed-only) benchmark for trcache.
 *
 * Simulates high-throughput feed, partitioning symbols across feed threads.
 * Each thread generates trades for its partition based on a Zipf distribution.
 * Measures pipeline performance and memory usage. No readers involved.
 * OS handles thread scheduling (no core pinning).
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

#include "trcache.h" /* Public trcache API, includes memstat_category */

/* Error logging macro */
#define errmsg(stream, fmt, ...)                         \
	do {                                                 \
		fprintf((stream), "[%s:%d:%s] " fmt,             \
		__FILE__, __LINE__, __func__, ##__VA_ARGS__);    \
	} while (0)

/* Constants */
#define NUM_SYMBOLS (4096)
#define NUM_CANDLE_TYPES (2)   /* 1 (Tick) + 1 (Time) */
#define DEFAULT_ZIPF_S (0.99) /* Default Zipf skewness */
#define ONE_MINUTE_MS (60000)   /* 1 minute = 60000 ms */

/* Candle Structure */
struct my_candle {
	struct trcache_candle_base base;
	double open;
	double high;
	double low;
	double close;
	double volume;
	double amount;
	uint32_t trade_count; /* Scratch space, not in field defs */
};

/* Field Definitions (excluding trade_count) */
const struct trcache_field_def g_candle_fields[] = {
	{offsetof(struct my_candle, open), sizeof(double),
		FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, high), sizeof(double),
		FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, low), sizeof(double),
		FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, close), sizeof(double),
		FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, volume), sizeof(double),
		FIELD_TYPE_DOUBLE},
	{offsetof(struct my_candle, amount), sizeof(double),
		FIELD_TYPE_DOUBLE},
};

/* Global Configuration */
struct benchmark_config {
	int num_feed_threads;
	int num_worker_threads;
	const char *output_csv_path;
	int total_time_sec;
	int warmup_time_sec;
	double zipf_s;
};

/**
 * @brief A cacheline-padded counter to prevent false sharing between
 * feed threads.
 */
struct padded_counter {
	uint64_t count;
	char padding[CACHE_LINE_SIZE - sizeof(uint64_t)];
} ____cacheline_aligned;


/* Global State */
static struct trcache *g_cache = NULL;
static struct benchmark_config g_config;
static _Atomic(bool) g_running = true;
static int g_symbol_ids[NUM_SYMBOLS];
static FILE *g_csv_file = NULL;
static pthread_mutex_t g_csv_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Array of per-thread counters, padded to prevent false sharing */
static struct padded_counter *g_thread_feed_counters = NULL;


/* Function Prototypes */
static void init_partition_zipf_generator(int start_rank, int num_symbols,
	double s, double *cdf_array);
static int get_next_partition_symbol_idx(int num_symbols,
	const double *cdf_array, unsigned int *rand_state);
static uint64_t get_total_feed_count(void);

/*
 * ====================================================================
 * Zipfian Distribution Generator (Partition Aware)
 * ====================================================================
 */

/**
 * @brief Initializes the Zipf CDF for a specific partition of symbols.
 * Ranks are 1-based. Handles s=0 for uniform distribution within partition.
 *
 * @param start_rank    The global rank of the first symbol in this partition.
 * @param num_symbols   Number of symbols in this partition.
 * @param s             Zipf exponent 's'.
 * @param cdf_array     Output array (size num_symbols) to store the relative CDF.
 */
static void init_partition_zipf_generator(int start_rank, int num_symbols,
	double s, double *cdf_array)
{
	if (num_symbols <= 0) return;

	/* Handle uniform distribution (s=0) */
	if (s == 0.0) {
		for (int i = 0; i < num_symbols; i++) {
			cdf_array[i] = (double)(i + 1) / num_symbols;
		}
		return;
	}

	/* Calculate normalization sum *for this partition* */
	double partition_sum = 0.0;
	for (int i = 0; i < num_symbols; i++) {
		partition_sum += 1.0 / pow((double)(start_rank + i), s);
	}

	/* Calculate relative CDF within the partition */
	double cumulative = 0.0;
	if (partition_sum > 0) { /* Avoid division by zero */
		for (int i = 0; i < num_symbols; i++) {
			cumulative += (1.0 / pow((double)(start_rank + i), s))
				/ partition_sum;
			cdf_array[i] = cumulative;
		}
	} else { /* Should only happen if s is extremely large and start_rank > 1 */
		for (int i = 0; i < num_symbols; i++) {
			cdf_array[i] = (double)(i + 1) / num_symbols; /* Fallback to uniform */
		}
	}
	/* Ensure the last element is exactly 1.0 */
	cdf_array[num_symbols - 1] = 1.0;
}

/**
 * @brief Returns a relative symbol index (0 to num_symbols-1) within a
 * partition based on the partition's pre-computed Zipf CDF.
 *
 * @param num_symbols Number of symbols in the partition/CDF array.
 * @param cdf_array   The pre-computed CDF for the partition.
 * @param rand_state  Pointer to the thread-local random state.
 * @return A relative symbol index (0 to num_symbols - 1).
 */
static int get_next_partition_symbol_idx(int num_symbols,
	const double *cdf_array, unsigned int *rand_state)
{
	if (num_symbols <= 0) return 0; /* Should not happen */

	/* Generate a random double between 0.0 and 1.0 */
	double u = (double)rand_r(rand_state) / (double)RAND_MAX;

	/* Binary search the partition's CDF */
	int lo = 0, hi = num_symbols - 1;
	while (lo < hi) {
		int mid = lo + ((hi - lo) >> 1);
		if (cdf_array[mid] < u) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo; /* Returns the relative index */
}


/*
 * ====================================================================
 * trcache Callback Definitions
 * ====================================================================
 */

/**
 * @brief Initialize a tick-based candle.
 *
 * Sets the key to the trade_id of the first trade.
 */
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

/**
 * @brief Macro to define a tick-based update function.
 * @param N The number of ticks to close the candle.
 */
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

/* Define the 3-tick update function */
DEFINE_TICK_UPDATE_FUNC(3)

/**
 * @brief Initialize a time-based candle (e.g., 1-minute).
 *
 * Sets the key to the start of the time interval based on
 * the trade's timestamp.
 */
static void candle_init_time(struct trcache_candle_base *c,
	void *data)
{
	struct trcache_trade_data *d = (struct trcache_trade_data *)data;
	struct my_candle *candle = (struct my_candle *)c;
	double price = d->price.as_double;
	double volume = d->volume.as_double;

	/* Set key to the start of the 1-minute interval */
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

/**
 * @brief Macro to define a time-based update function.
 * @param SUFFIX The function name suffix (e.g., 1min).
 * @param DURATION_MS The duration in milliseconds to close the candle.
 */
#define DEFINE_TIME_UPDATE_FUNC(SUFFIX, DURATION_MS) \
static bool candle_update_time_##SUFFIX(struct trcache_candle_base *c, \
	void *data) \
{ \
	struct trcache_trade_data *d = (struct trcache_trade_data *)data; \
	/* Check if trade is outside the [key, key + DURATION_MS) window */ \
	if (d->timestamp >= c->key.timestamp + (DURATION_MS)) { \
		c->is_closed = true; \
		return false; /* This trade starts a new candle */ \
	} \
	\
	/* Trade is within the window, aggregate it */ \
	struct my_candle *candle = (struct my_candle *)c; \
	double price = d->price.as_double; \
	double volume = d->volume.as_double; \
	if (price > candle->high) candle->high = price; \
	if (price < candle->low) candle->low = price; \
	candle->close = price; \
	candle->volume += volume; \
	candle->amount += price * volume; \
	candle->trade_count++; \
	\
	return true; /* Trade was consumed */ \
}

/* Define the 1-minute update function */
DEFINE_TIME_UPDATE_FUNC(1min, ONE_MINUTE_MS)


/**
 * @brief No-op flush function for the benchmark.
 */
static void* sync_flush_noop(struct trcache *cache,
	struct trcache_candle_batch *batch, void *ctx)
{
	(void)cache; (void)batch; (void)ctx;
	return NULL;
}

/*
 * ====================================================================
 * Benchmark Thread Functions
 * ====================================================================
 */

struct feed_thread_args {
	int thread_idx;
};

/**
 * @brief Main function for a single feed thread.
 * Initializes a Zipf generator for its partition, then generates and
 * feeds trades only for symbols within that partition.
 */
static void* feed_thread_main(void *arg)
{
	struct feed_thread_args *args = (struct feed_thread_args *)arg;
	/* Get this thread's dedicated, padded counter */
	struct padded_counter *my_counter =
		&g_thread_feed_counters[args->thread_idx];

	printf("  [Feed Thread %d] started.\n", args->thread_idx);

	/* Calculate partition for this thread */
	int num_feed_threads = g_config.num_feed_threads;
	int partition_size = NUM_SYMBOLS / num_feed_threads;
	int start_idx = args->thread_idx * partition_size;
	/* Handle remainder for the last thread */
	int end_idx = (args->thread_idx == num_feed_threads - 1) ?
		NUM_SYMBOLS : (args->thread_idx + 1) * partition_size;
	int symbols_in_partition = end_idx - start_idx;

	/* Allocate and initialize thread-local CDF for this partition */
	double *local_zipf_cdf = NULL;
	if (symbols_in_partition > 0) {
		local_zipf_cdf = malloc(symbols_in_partition * sizeof(double));
		if (local_zipf_cdf == NULL) {
			errmsg(stderr, "Feed thread %d failed to alloc CDF\n",
				args->thread_idx);
			return NULL; /* Exit thread */
		}
		/* Ranks are 1-based, index is 0-based */
		init_partition_zipf_generator(start_idx + 1, symbols_in_partition,
			g_config.zipf_s, local_zipf_cdf);
	}

	/* Initialize other thread-local state */
	struct trcache_trade_data trade;
	unsigned int rand_state = (unsigned int)(time(NULL) ^ pthread_self());
	uint64_t trade_id = 0;
	int symbol_idx = 0;      /* Absolute index (0 to NUM_SYMBOLS-1) */
	int relative_idx = 0;    /* Relative index within partition */
	int symbol_id = 0;

	/* Main feed loop */
	while (atomic_load_explicit(&g_running, memory_order_relaxed)) {

		/* Ensure partition is not empty */
		if (symbols_in_partition <= 0) {
			sched_yield(); /* Avoid busy-loop if no symbols assigned */
			continue;
		}

		/* Select a relative index within this thread's partition */
		relative_idx = get_next_partition_symbol_idx(symbols_in_partition,
			local_zipf_cdf, &rand_state);

		/* Convert relative index to absolute symbol index */
		symbol_idx = start_idx + relative_idx;

		/* Get the registered symbol ID */
		symbol_id = g_symbol_ids[symbol_idx];
		/* Get a unique trade ID */
		trade_id++;

		/* Generate pseudo-random trade data */
		trade.timestamp = (uint64_t)time(NULL) * 1000;
		trade.trade_id = trade_id;
		trade.price.as_double = 100.0
			+ ((double)rand_r(&rand_state) / RAND_MAX);
		trade.volume.as_double = 1.0
			+ ((double)rand_r(&rand_state) / RAND_MAX * 100.0);

		/* Feed the trade to trcache */
		while (trcache_feed_trade_data(g_cache, &trade, symbol_id) != 0) {
			if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
				goto feed_loop_exit;
			}

			sched_yield();
		}

		/* Increment this thread's local, non-atomic counter */
		my_counter->count++;
	}

feed_loop_exit:
	printf("  [Feed Thread %d] stopping.\n", args->thread_idx);
	free(local_zipf_cdf); /* Clean up thread-local CDF */
	return NULL;
}


/**
 * @brief Sums all per-thread feed counters.
 * This is non-atomic and assumes counters are only written by their
 * owning thread. The monitor thread reads them, so some reads might
 * be slightly stale, which is acceptable for this benchmark.
 * @return The total sum of all feed counters.
 */
static uint64_t get_total_feed_count(void)
{
	uint64_t total = 0;
	if (g_thread_feed_counters == NULL) {
		return 0;
	}

	for (int i = 0; i < g_config.num_feed_threads; i++) {
		/*
		 * Non-atomic read. Might be slightly stale, but
		 * good enough for monitoring.
		 */
		total += g_thread_feed_counters[i].count;
	}
	return total;
}


/* write_csv_header remains the same */
static void write_csv_header(void)
{
	fprintf(g_csv_file,
		"Timestamp,"         /* Unix timestamp */
		"ElapsedSec,"        /* Seconds since benchmark start */
		"FeedThreads,"       /* Config: Number of feed threads (N) */
		"WorkerThreads,"     /* Config: Number of worker threads (M) */
		"ZipfS,"             /* Config: Zipf exponent s */
		"FeedCountPerSec\n"  /* Throughput */
	);
	fflush(g_csv_file);
}

/* monitor_thread_main remains the same */
static void* monitor_thread_main(void *arg)
{
	(void)arg;
	printf("  [Monitor Thread] started.\n");

	g_csv_file = fopen(g_config.output_csv_path, "w");
	if (g_csv_file == NULL) {
		errmsg(stderr, "Failed to open output CSV file '%s': %s\n",
			g_config.output_csv_path, strerror(errno));
		atomic_store(&g_running, false);
		return NULL;
	}

	pthread_mutex_lock(&g_csv_mutex);
	write_csv_header();
	pthread_mutex_unlock(&g_csv_mutex);

	time_t start_time = time(NULL);
	time_t last_log_time = start_time;
	int elapsed_sec = 0;

	/*
	 * prev_feed_counter tracks the total count at the *previous*
	 * second. We initialize it to the count *now* (which should be 0).
	 */
	uint64_t prev_feed_counter = get_total_feed_count();
	uint64_t current_feed_counter = prev_feed_counter;

	while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
		time_t now = time(NULL);
		if (now == last_log_time) {
			usleep(100000);
			continue;
		}
		last_log_time = now;
		elapsed_sec = (int)(now - start_time);

		/* Get the current total feed count by summing all thread counters */
		current_feed_counter = get_total_feed_count();
		uint64_t feed_count_this_sec = current_feed_counter - prev_feed_counter;

		if (elapsed_sec <= g_config.warmup_time_sec) {
			if (elapsed_sec > 0 && elapsed_sec % 5 == 0) {
				printf("  [Monitor Thread] Warming up... %d/%d sec "
					"(Feed/sec: %lu)\n",
					elapsed_sec, g_config.warmup_time_sec,
					feed_count_this_sec);
			}
			/* Update baseline counter *during* warmup */
			prev_feed_counter = current_feed_counter;
			continue;
		}

		/*
		 * WARMUP IS OVER - START LOGGING
		 */

		pthread_mutex_lock(&g_csv_mutex);
		fprintf(g_csv_file,
			"%ld,%d,%d,%d,%.2f," /* Time, Elapsed, Threads, ZipfS */
			"%lu\n",             /* FeedCountPerSec */
			now, elapsed_sec, g_config.num_feed_threads,
			g_config.num_worker_threads, g_config.zipf_s,
			feed_count_this_sec
		);
		fflush(g_csv_file);
		pthread_mutex_unlock(&g_csv_mutex);

		printf("  [Monitor Thread] %d second, Feed/sec: %lu\n",
				elapsed_sec, feed_count_this_sec);
		
		/* Update baseline for next second's measurement */
		prev_feed_counter = current_feed_counter;
	}

	printf("  [Monitor Thread] stopping.\n");
	if (g_csv_file) {
		fclose(g_csv_file);
		g_csv_file = NULL;
	}
	return NULL;
}


/* initialize_trcache remains the same */
static int initialize_trcache(void)
{
	/* Define the update_ops structs for all candle types */
	const struct trcache_candle_update_ops g_update_ops[NUM_CANDLE_TYPES] = {
		[0] = { .init = candle_init_tick, .update = candle_update_tick_3 },
		[1] = { .init = candle_init_time, .update = candle_update_time_1min }
	};
	/* Define the shared no-op flush_ops */
	const struct trcache_batch_flush_ops g_flush_ops = {
		.flush = sync_flush_noop
	};

	/* Define NUM_FIELDS here for clarity */
	const int num_fields = sizeof(g_candle_fields)
		/ sizeof(struct trcache_field_def);
	const size_t candle_size = sizeof(struct my_candle);

	/* Initialize the candle configuration array at declaration time */
	struct trcache_candle_config configs[NUM_CANDLE_TYPES] = {
		{ /* [0] - 3 Tick */
			.user_candle_size = candle_size,
			.field_definitions = g_candle_fields,
			.num_fields = num_fields,
			.update_ops = g_update_ops[0],
			.flush_ops = g_flush_ops,
		},
		{ /* [1] - 1 Minute */
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
			trcache_destroy(g_cache); g_cache = NULL;
			return -1;
		}
	}
	return 0;
}

/* print_usage remains mostly the same, just removing core list help */
static void print_usage(const char *prog_name)
{
	fprintf(stderr,
		"Usage: %s [options]\n\n"
		"Required Options:\n"
		"  -f, --feed-threads <N>     Number of feed producer threads\n"
		"  -w, --worker-threads <M>   Number of trcache internal workers\n"
		"  -o, --output-csv <path>    Path to output CSV file\n"
		"  -t, --total-time <sec>     Total experiment duration (seconds)\n"
		"  -W, --warmup-time <sec>    Warmup duration (seconds)\n"
		"\n"
		"Optional Options:\n"
		"  -s, --zipf-s <value>       Zipf exponent 's' (>= 0). s=0 means\n"
		"                             even distribution. (default: %.2f)\n"
		/* Core list options removed */
		"  -h, --help                 Print this help message\n",
		prog_name, DEFAULT_ZIPF_S
	);
}

/* parse_arguments remains mostly the same, removing core list options */
static int parse_arguments(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	g_config.zipf_s = DEFAULT_ZIPF_S;

	const struct option long_options[] = {
		{"feed-threads", required_argument, 0, 'f'},
		{"worker-threads", required_argument, 0, 'w'},
		{"output-csv", required_argument, 0, 'o'},
		{"total-time", required_argument, 0, 't'},
		{"warmup-time", required_argument, 0, 'W'},
		{"zipf-s", required_argument, 0, 's'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "f:w:o:t:W:s:h", /* F,C removed */
		long_options, NULL)) != -1) {
		switch (c) {
			case 'f': g_config.num_feed_threads = atoi(optarg); break;
			case 'w': g_config.num_worker_threads = atoi(optarg); break;
			case 'o': g_config.output_csv_path = optarg; break;
			case 't': g_config.total_time_sec = atoi(optarg); break;
			case 'W': g_config.warmup_time_sec = atoi(optarg); break;
			case 's':
				g_config.zipf_s = atof(optarg);
				if (g_config.zipf_s < 0.0) {
					errmsg(stderr, "Zipf exponent 's' must be >= 0\n");
					return -1;
				}
				break;
			case 'h': print_usage(argv[0]); return -1;
			case '?': default: return -1;
		}
	}

	if (g_config.num_feed_threads <= 0 || g_config.num_worker_threads <= 0 ||
		!g_config.output_csv_path || g_config.total_time_sec <= 0 ||
		g_config.warmup_time_sec < 0) {
		errmsg(stderr, "Missing or invalid required arguments\n\n");
		print_usage(argv[0]);
		return -1;
	}
	if (g_config.warmup_time_sec >= g_config.total_time_sec) {
		errmsg(stderr, "Warmup time (%ds) must be < total time (%ds)\n",
			g_config.warmup_time_sec, g_config.total_time_sec);
		return -1;
	}
	return 0;
}


/* Main Function */
int main(int argc, char **argv)
{
	pthread_t monitor_thread;
	pthread_t *feed_threads = NULL;
	struct feed_thread_args *feed_args = NULL;
	int ret_code = EXIT_SUCCESS;

	printf("trcache Feed-Only Benchmark\n");
	printf("-------------------------------\n");

	if (parse_arguments(argc, argv) != 0) {
		return EXIT_FAILURE;
	}

	/* Allocate memory for per-thread feed counters */
	g_thread_feed_counters = calloc(g_config.num_feed_threads,
		sizeof(struct padded_counter));
	if (g_thread_feed_counters == NULL) {
		errmsg(stderr, "Failed to allocate memory for feed counters\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	printf("Configuration:\n");
	printf("  Feed Threads:   %d\n", g_config.num_feed_threads);
	printf("  Worker Threads: %d\n", g_config.num_worker_threads);
	printf("  Output CSV:     %s\n", g_config.output_csv_path);
	printf("  Total Time:     %ds\n", g_config.total_time_sec);
	printf("  Warmup Time:    %ds\n", g_config.warmup_time_sec);
	printf("  Zipf Exponent (s): %.2f %s\n", g_config.zipf_s,
		(g_config.zipf_s == 0.0) ? "(Even Distribution)" : "");
	printf("-------------------------------\n");
	printf("Initializing trcache...\n");
	if (initialize_trcache() != 0) {
		errmsg(stderr, "Benchmark failed during trcache initialization\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}
	printf("trcache initialized successfully.\n");

	feed_threads = calloc(g_config.num_feed_threads, sizeof(pthread_t));
	feed_args = calloc(g_config.num_feed_threads,
		sizeof(struct feed_thread_args));
	if (feed_threads == NULL || feed_args == NULL) {
		errmsg(stderr, "Failed to allocate memory for feed threads\n");
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	atomic_store(&g_running, true);

	printf("Starting monitor thread...\n");
	if (pthread_create(&monitor_thread, NULL, monitor_thread_main, NULL)
		!= 0) {
		errmsg(stderr, "Failed to create monitor thread: %s\n",
			strerror(errno));
		ret_code = EXIT_FAILURE;
		goto cleanup;
	}

	printf("Starting %d feed threads...\n", g_config.num_feed_threads);
	bool feed_thread_error = false;
	for (int i = 0; i < g_config.num_feed_threads; i++) {
		feed_args[i].thread_idx = i;
		if (pthread_create(&feed_threads[i], NULL, feed_thread_main,
			&feed_args[i]) != 0) {
			errmsg(stderr, "Failed to create feed thread %d: %s\n", i,
				strerror(errno));
			atomic_store(&g_running, false);
			feed_thread_error = true;
			/* Join threads that were successfully created before the error */
			for (int j = 0; j < i; j++) {
				pthread_join(feed_threads[j], NULL);
			}
			pthread_join(monitor_thread, NULL); /* Join monitor thread */
			ret_code = EXIT_FAILURE;
			goto cleanup;
		}
	}

	printf("Benchmark running for %ds (warmup %ds)...\n",
		g_config.total_time_sec, g_config.warmup_time_sec);

	sleep(g_config.total_time_sec);

	printf("Benchmark time elapsed. Signaling threads to stop...\n");
	atomic_store(&g_running, false);

	/* Join threads only if they were all successfully created */
	if (!feed_thread_error) {
		for (int i = 0; i < g_config.num_feed_threads; i++) {
			pthread_join(feed_threads[i], NULL);
		}
		printf("All feed threads joined.\n");
		pthread_join(monitor_thread, NULL);
		printf("Monitor thread joined.\n");
	}

cleanup:
	printf("Cleaning up resources...\n");
	if (g_cache) {
		printf("Destroying trcache...\n");
		trcache_destroy(g_cache);
		g_cache = NULL;
	} else {
		printf("trcache was not initialized or already destroyed.\n");
	}
	free(g_thread_feed_counters);
	free(feed_threads);
	free(feed_args);

	if (g_csv_file) {
		fclose(g_csv_file);
		g_csv_file = NULL;
	}

	printf("Benchmark finished %s.\n", (ret_code == EXIT_SUCCESS) ?
		"successfully" : "with errors");
	if (ret_code == EXIT_SUCCESS && g_config.output_csv_path) {
		printf("Results saved to: %s\n", g_config.output_csv_path);
	}

	return ret_code;
}
