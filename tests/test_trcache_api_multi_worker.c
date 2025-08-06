#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include "trcache.h"

#define TRADES_PER_CANDLE 100
#define PRICE_BASE 100.0

static int g_total_seconds = 10;
static int g_num_workers = 1;
static int g_num_symbols = 1;
static struct timespec g_end_time;

static trcache *g_tc;
static int *g_symbol_ids;
static uint64_t *g_trade_ids;
static _Atomic int producer_done = 0;

struct thread_perf {
	struct timespec start;
	struct timespec end;
	unsigned long ops;
};

static double timespec_to_sec(const struct timespec *ts)
{
	return ts->tv_sec + ts->tv_nsec / 1e9;
}

static void perf_start(struct thread_perf *p)
{
	clock_gettime(CLOCK_MONOTONIC, &p->start);
	p->ops = 0;
}

static void perf_end(struct thread_perf *p)
{
	clock_gettime(CLOCK_MONOTONIC, &p->end);
}

static void *producer_thread(void *arg)
{
	struct thread_perf *perf = (struct thread_perf *)arg;
	perf_start(perf);

	while (1) {
		struct timespec now;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec > g_end_time.tv_sec ||
		    (now.tv_sec == g_end_time.tv_sec &&
		     now.tv_nsec >= g_end_time.tv_nsec))
			break;

		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		uint64_t now_ms = ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000ULL;

		for (int s = 0; s < g_num_symbols; s++) {
			uint64_t tid = g_trade_ids[s]++;
			struct trcache_trade_data td = {
				.timestamp = now_ms,
				.trade_id = tid,
				.price = PRICE_BASE + tid,
				.volume = 1.0
			};
			int ret = trcache_feed_trade_data(g_tc, &td,
							 g_symbol_ids[s]);
			assert(ret == 0);
			perf->ops++;
		}
	}

	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	uint64_t now_ms = ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000ULL;
	for (int s = 0; s < g_num_symbols; s++) {
		uint64_t tid = g_trade_ids[s]++;
		struct trcache_trade_data td = {
			.timestamp = now_ms,
			.trade_id = tid,
			.price = PRICE_BASE + tid,
			.volume = 1.0
		};
		trcache_feed_trade_data(g_tc, &td, g_symbol_ids[s]);
		perf->ops++;
	}
	perf_end(perf);
	atomic_store(&producer_done, 1);
	return NULL;
}

static void check_tick_candle(const struct trcache_candle_batch *b, int idx)
{
	uint64_t base = b->start_trade_id_array[idx];
	double open = b->open_array[idx];
	double close = b->close_array[idx];
	double high = b->high_array[idx];
	double low = b->low_array[idx];
	double volume = b->volume_array[idx];

	assert(base % TRADES_PER_CANDLE == 0);
	assert(volume == (double)TRADES_PER_CANDLE);

	double expected_open = PRICE_BASE + base;
	double expected_close = expected_open + volume - 1;

	assert(open == expected_open);
	assert(low == expected_open);
	assert(high == expected_close);
	assert(close == expected_close);
}

static void check_sec_candle(const struct trcache_candle_batch *b, int idx)
{
	uint64_t ts = b->start_timestamp_array[idx];
	uint64_t base_trade = b->start_trade_id_array[idx];
	double open = b->open_array[idx];
	double close = b->close_array[idx];
	double high = b->high_array[idx];
	double low = b->low_array[idx];
	double volume = b->volume_array[idx];

	assert(ts % 1000 == 0);

	double expected_open = PRICE_BASE + base_trade;
	double expected_close = expected_open + volume - 1;

	assert(open == expected_open);
	assert(low == expected_open);
	assert(high == expected_close);
	assert(close == expected_close);
}

static void *reader_tick_thread(void *arg)
{
	struct thread_perf *perf = (struct thread_perf *)arg;
	struct trcache_candle_batch *batch =
		trcache_batch_alloc_on_heap(5, TRCACHE_FIELD_MASK_ALL);
	trcache_candle_field_flags mask = TRCACHE_START_TRADE_ID |
		TRCACHE_START_TIMESTAMP | TRCACHE_OPEN | TRCACHE_HIGH |
		TRCACHE_LOW | TRCACHE_CLOSE | TRCACHE_VOLUME;
	perf_start(perf);
	unsigned int seed = (unsigned int)time(NULL) ^ (uintptr_t)pthread_self();

	while (!atomic_load(&producer_done)) {
		int idx = rand_r(&seed) % g_num_symbols;
		if (trcache_get_candles_by_symbol_id_and_offset(
				g_tc, g_symbol_ids[idx],
				TRCACHE_100TICK_CANDLE, mask, 1, 5, batch) == 0) {
			for (int i = 0; i < batch->num_candles; i++)
				check_tick_candle(batch, i);
			perf->ops++;
		} else {
			sched_yield();
		}
	}

	perf_end(perf);
	trcache_batch_free(batch);
	return NULL;
}

static void *reader_sec_thread(void *arg)
{
	struct thread_perf *perf = (struct thread_perf *)arg;
	struct trcache_candle_batch *batch =
		trcache_batch_alloc_on_heap(5, TRCACHE_FIELD_MASK_ALL);
	trcache_candle_field_flags mask = TRCACHE_START_TRADE_ID |
		TRCACHE_START_TIMESTAMP | TRCACHE_OPEN | TRCACHE_HIGH |
		TRCACHE_LOW | TRCACHE_CLOSE | TRCACHE_VOLUME;
	perf_start(perf);
	sleep(5);
	unsigned int seed = (unsigned int)time(NULL) ^ (uintptr_t)pthread_self();

	while (!atomic_load(&producer_done)) {
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		uint64_t ms = now.tv_sec * 1000ULL + now.tv_nsec / 1000000ULL;
		uint64_t ts = (ms / 1000ULL) * 1000ULL;
		int idx = rand_r(&seed) % g_num_symbols;

		if (trcache_get_candles_by_symbol_id_and_ts(
				g_tc, g_symbol_ids[idx],
				TRCACHE_1SEC_CANDLE, mask, ts, 5, batch) == 0) {
			for (int i = 0; i < batch->num_candles; i++)
				check_sec_candle(batch, i);
			perf->ops++;
		} else {
			sched_yield();
		}
	}

	perf_end(perf);
	trcache_batch_free(batch);
	return NULL;
}

static void *dummy_flush(trcache *cache, trcache_candle_batch *batch, void *ctx)
{ (void)cache; (void)batch; (void)ctx; return NULL; }
static bool dummy_is_done(trcache *cache, trcache_candle_batch *batch, void *h)
{ (void)cache; (void)batch; (void)h; return true; }
static void dummy_destroy(void *h, void *ctx) { (void)h; (void)ctx; }

int main(int argc, char **argv)
{
	if (argc > 1)
		g_total_seconds = atoi(argv[1]);
	if (argc > 2)
		g_num_workers = atoi(argv[2]);
	if (argc > 3)
		g_num_symbols = atoi(argv[3]);

	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC, &now);
	g_end_time = now;
	g_end_time.tv_sec += g_total_seconds;

	struct trcache_init_ctx init_ctx = {
		.num_worker_threads = g_num_workers,
		.batch_candle_count_pow2 = 12,
		.flush_threshold_pow2 = 0,
		.candle_type_flags = TRCACHE_100TICK_CANDLE | TRCACHE_1SEC_CANDLE,
		.flush_ops = {
			.flush = dummy_flush,
			.is_done = dummy_is_done,
			.destroy_handle = dummy_destroy,
			.flush_ctx = NULL,
			.destroy_handle_ctx = NULL,
		},
	};

	g_tc = trcache_init(&init_ctx);
	assert(g_tc != NULL);

	g_symbol_ids = calloc(g_num_symbols, sizeof(int));
	g_trade_ids = calloc(g_num_symbols, sizeof(uint64_t));
	assert(g_symbol_ids && g_trade_ids);

	int dummy_id = trcache_register_symbol(g_tc, "dummy");
	(void)dummy_id;
	char name[16];
	for (int i = 0; i < g_num_symbols; i++) {
		snprintf(name, sizeof(name), "sym%d", i);
		g_symbol_ids[i] = trcache_register_symbol(g_tc, name);
		assert(g_symbol_ids[i] >= 0);
		g_trade_ids[i] = 0;
	}

	pthread_t prod_t, read_tick_t, read_sec_t;
	struct thread_perf prod_perf, read_tick_perf, read_sec_perf;

	pthread_create(&prod_t, NULL, producer_thread, &prod_perf);
	//pthread_create(&read_tick_t, NULL, reader_tick_thread, &read_tick_perf);
	//pthread_create(&read_sec_t, NULL, reader_sec_thread, &read_sec_perf);

	pthread_join(prod_t, NULL);
	//pthread_join(read_tick_t, NULL);
	//pthread_join(read_sec_t, NULL);

	trcache_destroy(g_tc);
	free(g_symbol_ids);
	free(g_trade_ids);

	double prod_time = timespec_to_sec(&prod_perf.end) -
			   timespec_to_sec(&prod_perf.start);
	double read_tick_time = timespec_to_sec(&read_tick_perf.end) -
				timespec_to_sec(&read_tick_perf.start);
	double read_sec_time = timespec_to_sec(&read_sec_perf.end) -
			       timespec_to_sec(&read_sec_perf.start);

	printf("Producer: %.2f ops/sec\n", prod_perf.ops / prod_time);
	printf("Reader(tick) loops: %.2f/sec\n", read_tick_perf.ops / read_tick_time);
	printf("Reader(sec) loops: %.2f/sec\n", read_sec_perf.ops / read_sec_time);

	return 0;
}

