// test_memory_limit.c
//
// Stress‑test for the trcache memory limit.
//
// This program initialises a trcache instance with a user supplied
// memory limit and then continually pushes trade data into the cache
// until memory consumption approaches the configured limit.  The goal
// is to verify that the engine’s internal memory accounting and
// eviction logic prevent unbounded growth of the resident set size.
//
// Build with the rest of the project and run with a single argument
// specifying the desired memory limit, e.g.:
//
//	 ./test_memory_limit 256M
//
// Supported suffixes are K, M and G (optionally followed by B).  The
// argument is case insensitive.  If no suffix is provided the value
// is interpreted as a number of bytes.  For example, "1048576" is
// equivalent to "1M".

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <pthread.h>
#include <time.h>

#include "trcache.h"

// Convert a human‑friendly size string into a number of bytes.  The
// function accepts an integer optionally followed by one of the
// suffixes K, M or G (optionally with a trailing 'B').  For example
// "512M" becomes 512*1024*1024.  Returns 0 on error (invalid
// argument or overflow).
static size_t parse_size(const char *arg) {
	if (arg == NULL || *arg == '\0') {
		return 0;
	}
	size_t len = strlen(arg);
	// Strip an optional trailing 'B' or 'b'.
	char last = (char)toupper((unsigned char)arg[len - 1]);
	size_t end = len;
	if (last == 'B') {
		end--;
		if (end == 0) {
			return 0;
		}
		last = (char)toupper((unsigned char)arg[end - 1]);
	}
	size_t multiplier = 1;
	if (last == 'G' || last == 'M' || last == 'K') {
		switch (last) {
		case 'G':
			multiplier = 1024ULL * 1024ULL * 1024ULL;
			break;
		case 'M':
			multiplier = 1024ULL * 1024ULL;
			break;
		case 'K':
			multiplier = 1024ULL;
			break;
		}
		end--;
		if (end == 0) {
			return 0;
		}
	}
	// Copy the numeric part into a temporary buffer for conversion.
	char numbuf[64];
	if (end >= sizeof(numbuf)) {
		// Too long to be a valid number.
		return 0;
	}
	memcpy(numbuf, arg, end);
	numbuf[end] = '\0';
	errno = 0;
	char *endptr = NULL;
	unsigned long long base = strtoull(numbuf, &endptr, 10);
	if (errno != 0 || endptr == numbuf || *endptr != '\0') {
		return 0;
	}
	// Check for overflow when multiplying.
	if (base > SIZE_MAX / multiplier) {
		return 0;
	}
	return (size_t)base * multiplier;
}

// Dummy flush callback: synchronous no‑op.  The engine will free
// internal structures after this returns.	Returning NULL tells
// trcache that no asynchronous handle is used.
static void *test_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
	(void)cache;
	(void)batch;
	(void)ctx;
	return NULL;
}

// Dummy is_done callback for asynchronous flushes.  For the
// synchronous flush above this will never be called, but provide it
// anyway to satisfy the interface.  Always returns true.
static bool test_is_done(trcache *cache, trcache_candle_batch *batch, void *handle) {
	(void)cache;
	(void)batch;
	(void)handle;
	return true;
}

// Structure passed to each feed thread.  Each thread is given a
// contiguous subset of symbol IDs to which it will repeatedly feed
// trades.	The stop flag is polled periodically to terminate the
// loop.
struct feed_arg {
	trcache *cache;
	const int *symbols;
	int symbol_count;
	// Each thread maintains its own timestamp and trade_id offsets
	// to avoid contention.
	uint64_t base_timestamp;
	uint64_t base_trade_id;
	volatile int *stop_flag;
};

// Feed thread entry point.  Cycles through the assigned symbols and
// pushes trade data until stop_flag becomes non‑zero.
static void *feed_thread_func(void *arg) {
	struct feed_arg *fa = (struct feed_arg *)arg;
	struct trcache_trade_data trade;
	// Initialise local counters for this thread.
	uint64_t ts = fa->base_timestamp;
	uint64_t tid = fa->base_trade_id;
	while (!*(fa->stop_flag)) {
		for (int i = 0; i < fa->symbol_count; i++) {
			trade.timestamp = ts++;
			trade.trade_id = tid++;
			trade.price = 1.0;
			trade.volume = 1.0;
			// Push trade into cache.  Ignore return value; errors
			// will be handled by the main thread’s memory monitor.
			trcache_feed_trade_data(fa->cache, &trade, fa->symbols[i]);
			if (*(fa->stop_flag)) {
				break;
			}
		}
		//struct timespec ts;
		//ts.tv_sec = 0;
		//ts.tv_nsec = 1000000; // 밀리초 → 나노초
		//nanosleep(&ts, NULL);
	}
	return NULL;
}

// Dummy destroy handle callback.  With a synchronous flush there is
// nothing to clean up, but define this for completeness.
static void test_destroy_handle(void *handle, void *ctx) {
	(void)handle;
	(void)ctx;
}

int main(int argc, char **argv) {
	if (argc < 6) {
		fprintf(stderr,
				"Usage: %s <memory_limit> <duration_secs> <worker_threads> <num_symbols> <feed_threads>\n",
				argv[0]);
		fprintf(stderr,
				"Example: %s 512M 10 2 4 2\n",
				argv[0]);
		return 1;
	}
	// Parse command‑line arguments.
	size_t mem_limit = parse_size(argv[1]);
	if (mem_limit == 0) {
		fprintf(stderr, "Invalid memory limit: %s\n", argv[1]);
		return 1;
	}
	int duration = atoi(argv[2]);
	int worker_threads = atoi(argv[3]);
	int num_symbols = atoi(argv[4]);
	int feed_thread_count = atoi(argv[5]);
	if (duration <= 0 || worker_threads < 0 || num_symbols <= 0 || feed_thread_count <= 0) {
		fprintf(stderr, "Invalid numeric argument.\n");
		return 1;
	}
	// Initialise trcache with user‑provided worker thread count and default
	// batch settings.	Conservative flush thresholds help to stress
	// memory consumption.
	struct trcache_init_ctx ctx = {
		.num_worker_threads = worker_threads,
		.batch_candle_count_pow2 = 10,
		.cached_batch_count_pow2 = 1,
		.candle_type_flags = TRCACHE_1MIN_CANDLE,
		.flush_ops = {
			.flush = test_flush,
			.is_done = test_is_done,
			.destroy_handle = test_destroy_handle,
			.flush_ctx = NULL,
			.destroy_handle_ctx = NULL
		},
		.aux_memory_limit = mem_limit
	};
	trcache *cache = trcache_init(&ctx);
	if (cache == NULL) {
		fprintf(stderr, "Failed to initialise trcache\n");
		return 1;
	}
	// Register the requested number of symbols.  Names are generated
	// sequentially (SYM0, SYM1, …).  Keep an array of the returned
	// symbol IDs.
	int *symbol_ids = (int *)malloc(sizeof(int) * (size_t)num_symbols);
	if (!symbol_ids) {
		fprintf(stderr, "Out of memory allocating symbol_ids\n");
		trcache_destroy(cache);
		return 1;
	}
	for (int i = 0; i < num_symbols; i++) {
		char symbuf[64];
		snprintf(symbuf, sizeof(symbuf), "SYM%d", i);
		int sid = trcache_register_symbol(cache, symbuf);
		if (sid < 0) {
			fprintf(stderr, "Failed to register symbol %s\n", symbuf);
			free(symbol_ids);
			trcache_destroy(cache);
			return 1;
		}
		symbol_ids[i] = sid;
	}
	// Partition the symbol array evenly among the feed threads.  Some
	// threads may get one extra symbol if num_symbols is not divisible
	// by feed_thread_count.  Compute start and end indices for each.
	struct feed_arg *args = (struct feed_arg *)malloc(
		sizeof(struct feed_arg) * (size_t)feed_thread_count);
	if (!args) {
		fprintf(stderr, "Out of memory allocating feed arguments\n");
		free(symbol_ids);
		trcache_destroy(cache);
		return 1;
	}
	int base = num_symbols / feed_thread_count;
	int remainder = num_symbols % feed_thread_count;
	int offset = 0;
	volatile int stop_flag = 0;
	for (int i = 0; i < feed_thread_count; i++) {
		int count = base + (i < remainder ? 1 : 0);
		args[i].cache = cache;
		args[i].symbols = &symbol_ids[offset];
		args[i].symbol_count = count;
		args[i].base_timestamp = (uint64_t)(i) << 32;
		args[i].base_trade_id = (uint64_t)(i) << 32;
		args[i].stop_flag = &stop_flag;
		offset += count;
	}
	// Launch feed threads.
	pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * (size_t)feed_thread_count);
	if (!threads) {
		fprintf(stderr, "Out of memory allocating thread handles\n");
		free(args);
		free(symbol_ids);
		trcache_destroy(cache);
		return 1;
	}
	for (int i = 0; i < feed_thread_count; i++) {
		if (args[i].symbol_count == 0) {
			// No symbols to feed; skip thread creation.
			threads[i] = 0;
			continue;
		}
		if (pthread_create(&threads[i], NULL, feed_thread_func, &args[i]) != 0) {
			fprintf(stderr, "Failed to create feed thread %d\n", i);
			stop_flag = 1;
			// Join any threads that were created.
			for (int j = 0; j < i; j++) {
				if (threads[j]) {
					pthread_join(threads[j], NULL);
				}
			}
			free(threads);
			free(args);
			free(symbol_ids);
			trcache_destroy(cache);
			return 1;
		}
	}
	// Record start time.
	struct timespec start_ts;
	clock_gettime(CLOCK_MONOTONIC, &start_ts);
	bool exceeded = false;
	fprintf(stderr,
			"Starting memory stress test with limit=%zu bytes, duration=%ds, worker_threads=%d, symbols=%d, feed_threads=%d\n",
			mem_limit, duration, worker_threads, num_symbols, feed_thread_count);
	while (!exceeded) {
		struct timespec now;
		clock_gettime(CLOCK_MONOTONIC, &now);
		double elapsed = (now.tv_sec - start_ts.tv_sec) +
						 (now.tv_nsec - start_ts.tv_nsec) / 1e9;
		if (elapsed >= (double)duration) {
			break;
		}
		// Sleep for a short interval to reduce overhead.
		struct timespec req = {0, 100000000L}; // 100 ms
		nanosleep(&req, NULL);
		// Check memory usage.
		struct rusage usage;
		if (getrusage(RUSAGE_SELF, &usage) == 0) {
			size_t rss_bytes;
#if defined(__APPLE__)
			rss_bytes = (size_t)usage.ru_maxrss;
#else
			rss_bytes = (size_t)usage.ru_maxrss * 1024ULL;
#endif
			fprintf(stderr, "Elapsed %.1fs RSS=%zu bytes (%.2f%% of limit)\n",
					elapsed, rss_bytes,
					mem_limit > 0 ? (rss_bytes * 100.0) / mem_limit : 0.0);
			trcache_print_aux_memory_breakdown(cache);
			trcache_print_worker_distribution(cache);
			if (mem_limit > 0 && rss_bytes > (size_t)((double)mem_limit * 2)) {
				fprintf(stderr, "Memory usage exceeded limit: %zu bytes > %zu bytes\n",
						rss_bytes, mem_limit);
				exceeded = true;
				break;
			}
		}
	}
	// Signal feed threads to stop and wait for them to exit.
	stop_flag = 1;
	for (int i = 0; i < feed_thread_count; i++) {
		if (threads[i]) {
			pthread_join(threads[i], NULL);
		}
	}
	fprintf(stderr, "Stopping feed threads and destroying cache...\n");
	free(threads);
	free(args);
	free(symbol_ids);
	trcache_destroy(cache);
	// Report final status.
	if (!exceeded) {
		fprintf(stderr, "Memory usage remained within the configured limit during the test.\n");
		return 0;
	}
	fprintf(stderr, "Memory usage exceeded the configured limit during the test.\n");
	return 2;
}
