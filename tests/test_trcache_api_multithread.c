#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include "meta/trcache_internal.h"
#include "meta/symbol_table.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "trcache.h"

#define TRADES_PER_CANDLE 100
#define TRADES_PER_SEC 1000
#define PRICE_BASE 100.0

static int g_total_seconds = 10;
static int g_num_trades;
static int g_num_candles;

static struct trcache tc;
static int g_symbol_id;
static struct symbol_entry *g_sym;
static struct trade_data_buffer *g_buf;
static struct candle_chunk_list *g_list_tick;
static struct candle_chunk_list *g_list_sec;
static _Atomic int producer_done = 0;
static _Atomic int consumers_done = 0;

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
        struct list_head free_list;
        INIT_LIST_HEAD(&free_list);
        perf_start(perf);

        for (int i = 0; i < g_num_trades; i++) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                uint64_t now_ms = ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000ULL;
                struct trcache_trade_data td = {
                        .timestamp = now_ms,
                        .trade_id = i,
                        .price = PRICE_BASE + i,
                        .volume = 1.0
                };
                int ret = trade_data_buffer_push(g_buf, &td, &free_list);
                assert(ret == 0);
                perf->ops++;
                usleep(1000);
        }

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t now_ms = ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000ULL;
        struct trcache_trade_data td = {
                .timestamp = now_ms,
                .trade_id = g_num_trades,
                .price = PRICE_BASE + g_num_trades,
                .volume = 1.0
        };
        trade_data_buffer_push(g_buf, &td, &free_list);
        perf->ops++;
        perf_end(perf);
        atomic_store(&producer_done, 1);
        return NULL;
}

static void *consumer_tick_thread(void *arg)
{
        struct thread_perf *perf = (struct thread_perf *)arg;
        struct trade_data_buffer_cursor *cursor =
                trade_data_buffer_get_cursor(g_buf, TRCACHE_100TICK_CANDLE);
        int consumed = 0;
        perf_start(perf);

        while (consumed != g_num_trades + 1) {
                struct trcache_trade_data *array = NULL;
                int count = 0;
                if (trade_data_buffer_peek(g_buf, cursor, &array, &count)) {
                        for (int i = 0; i < count; i++) {
                                candle_chunk_list_apply_trade(g_list_tick, &array[i]);
                        }
                        trade_data_buffer_consume(g_buf, cursor, count);
                        consumed += count;
                        perf->ops += count;
                } else if (atomic_load(&producer_done) && consumed >= g_num_trades + 1) {
                        break;
                }
        }

        perf_end(perf);
        atomic_fetch_add(&consumers_done, 1);
        return NULL;
}

static void *consumer_sec_thread(void *arg)
{
        struct thread_perf *perf = (struct thread_perf *)arg;
        struct trade_data_buffer_cursor *cursor =
                trade_data_buffer_get_cursor(g_buf, TRCACHE_1SEC_CANDLE);
        int consumed = 0;
        perf_start(perf);

        while (consumed != g_num_trades + 1) {
                struct trcache_trade_data *array = NULL;
                int count = 0;
                if (trade_data_buffer_peek(g_buf, cursor, &array, &count)) {
                        for (int i = 0; i < count; i++) {
                                candle_chunk_list_apply_trade(g_list_sec, &array[i]);
                        }
                        trade_data_buffer_consume(g_buf, cursor, count);
                        consumed += count;
                        perf->ops += count;
                } else if (atomic_load(&producer_done) && consumed >= g_num_trades + 1) {
                        break;
                }
        }

        perf_end(perf);
        atomic_fetch_add(&consumers_done, 1);
        return NULL;
}

static void *convert_thread(void *arg)
{
        struct thread_perf *perf = (struct thread_perf *)arg;
        perf_start(perf);

        while (atomic_load(&consumers_done) < 2) {
                candle_chunk_list_convert_to_column_batch(g_list_tick);
                candle_chunk_list_convert_to_column_batch(g_list_sec);
                perf->ops += 2;
        }

        perf_end(perf);
        return NULL;
}

static void *flush_thread(void *arg)
{
        struct thread_perf *perf = (struct thread_perf *)arg;
        perf_start(perf);

        while (atomic_load(&consumers_done) < 2) {
                candle_chunk_list_flush(g_list_tick);
                candle_chunk_list_flush(g_list_sec);
                perf->ops += 2;
        }

        perf_end(perf);
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

        while (atomic_load(&consumers_done) < 2) {
                if (atomic_load(&g_list_tick->mutable_seq) != UINT64_MAX &&
                    atomic_load(&g_list_tick->mutable_seq) > 5 &&
                    trcache_get_candles_by_symbol_id_and_offset(&tc, g_symbol_id,
                                TRCACHE_100TICK_CANDLE, mask, 1, 5, batch) == 0) {
                        for (int i = 0; i < batch->num_candles; i++)
                                check_tick_candle(batch, i);
                        perf->ops++;
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

        while (atomic_load(&consumers_done) < 2) {
                if (atomic_load(&g_list_sec->mutable_seq) != UINT64_MAX &&
                    atomic_load(&g_list_sec->mutable_seq) > 5) {
                        struct timespec now;
                        clock_gettime(CLOCK_REALTIME, &now);
                        uint64_t ms = now.tv_sec * 1000ULL + now.tv_nsec / 1000000ULL;
                        uint64_t ts = (ms / 1000ULL) * 1000ULL;

                        if (trcache_get_candles_by_symbol_id_and_ts(&tc, g_symbol_id,
                                        TRCACHE_1SEC_CANDLE, mask, ts, 5, batch) == 0) {
                                for (int i = 0; i < batch->num_candles; i++)
                                        check_sec_candle(batch, i);
                                perf->ops++;
                        }
                }
        }

        perf_end(perf);
        trcache_batch_free(batch);
        return NULL;
}

static void *dummy_flush(trcache *cache, trcache_candle_batch *batch, void *ctx)
{
        (void)cache; (void)batch; (void)ctx; return NULL;
}
static bool dummy_is_done(trcache *cache, trcache_candle_batch *batch, void *h)
{
        (void)cache; (void)batch; (void)h; return true;
}
static void dummy_destroy(void *h, void *ctx) { (void)h; (void)ctx; }

int main(int argc, char **argv)
{
        if (argc > 1)
                g_total_seconds = atoi(argv[1]);
        g_num_trades = g_total_seconds * TRADES_PER_SEC;
        g_num_candles = g_num_trades / TRADES_PER_CANDLE;



        tc.candle_type_flags = TRCACHE_100TICK_CANDLE | TRCACHE_1SEC_CANDLE;
        tc.num_candle_types = 2;
        tc.batch_candle_count_pow2 = 10;
        tc.batch_candle_count = 1 << tc.batch_candle_count_pow2;
        tc.flush_threshold_pow2 = 1;
        tc.flush_threshold = 1 << tc.flush_threshold_pow2;
        tc.flush_ops.flush = dummy_flush;
        tc.flush_ops.is_done = dummy_is_done;
        tc.flush_ops.destroy_handle = dummy_destroy;

        tc.symbol_table = symbol_table_init(8);
        assert(tc.symbol_table != NULL);
        g_symbol_id = symbol_table_register(&tc, tc.symbol_table, "sym0");
        assert(g_symbol_id >= 0);
        g_sym = symbol_table_lookup_entry(tc.symbol_table, g_symbol_id);
        assert(g_sym != NULL);

        g_buf = g_sym->trd_buf;
        g_list_tick = g_sym->candle_chunk_list_ptrs[__builtin_ctz(TRCACHE_100TICK_CANDLE)];
        g_list_sec = g_sym->candle_chunk_list_ptrs[__builtin_ctz(TRCACHE_1SEC_CANDLE)];

        pthread_t prod_t, cons_tick_t, cons_sec_t, conv_t, flush_t, read_t, read_sec_t;
        struct thread_perf prod_perf, cons_tick_perf, cons_sec_perf,
                        conv_perf, flush_perf, read_perf, read_sec_perf;

        pthread_create(&prod_t, NULL, producer_thread, &prod_perf);
        pthread_create(&cons_tick_t, NULL, consumer_tick_thread, &cons_tick_perf);
        pthread_create(&cons_sec_t, NULL, consumer_sec_thread, &cons_sec_perf);
        pthread_create(&conv_t, NULL, convert_thread, &conv_perf);
        pthread_create(&flush_t, NULL, flush_thread, &flush_perf);
        pthread_create(&read_t, NULL, reader_tick_thread, &read_perf);
        pthread_create(&read_sec_t, NULL, reader_sec_thread, &read_sec_perf);

        pthread_join(prod_t, NULL);
        pthread_join(cons_tick_t, NULL);
        pthread_join(cons_sec_t, NULL);
        pthread_join(conv_t, NULL);
        pthread_join(flush_t, NULL);
        pthread_join(read_t, NULL);
        pthread_join(read_sec_t, NULL);

        symbol_table_destroy(tc.symbol_table);

        double prod_time = timespec_to_sec(&prod_perf.end) - timespec_to_sec(&prod_perf.start);
        double cons_tick_time = timespec_to_sec(&cons_tick_perf.end) - timespec_to_sec(&cons_tick_perf.start);
        double cons_sec_time = timespec_to_sec(&cons_sec_perf.end) - timespec_to_sec(&cons_sec_perf.start);
        double conv_time = timespec_to_sec(&conv_perf.end) - timespec_to_sec(&conv_perf.start);
        double flush_time = timespec_to_sec(&flush_perf.end) - timespec_to_sec(&flush_perf.start);
        double read_time = timespec_to_sec(&read_perf.end) - timespec_to_sec(&read_perf.start);
        double read_sec_time = timespec_to_sec(&read_sec_perf.end) - timespec_to_sec(&read_sec_perf.start);

        printf("Producer: %.2f ops/sec\n", prod_perf.ops / prod_time);
        printf("Consumer(tick): %.2f ops/sec\n", cons_tick_perf.ops / cons_tick_time);
        printf("Consumer(sec): %.2f ops/sec\n", cons_sec_perf.ops / cons_sec_time);
        printf("Converter loops: %.2f/sec\n", conv_perf.ops / conv_time);
        printf("Flusher loops: %.2f/sec\n", flush_perf.ops / flush_time);
        printf("Reader(tick) loops: %.2f/sec\n", read_perf.ops / read_time);
        printf("Reader(sec) loops: %.2f/sec\n", read_sec_perf.ops / read_sec_time);

        return 0;
}

