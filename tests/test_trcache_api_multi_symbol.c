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
#define PRICE_BASE 100.0

static int g_total_seconds = 10;
static int g_num_consumers = 1;
static int g_num_symbols = 1;
static struct timespec g_end_time;

static struct trcache tc;
static int *g_symbol_ids;
static struct symbol_entry **g_syms;
static struct trade_data_buffer **g_bufs;
static struct candle_chunk_list **g_list_ticks;
static struct candle_chunk_list **g_list_secs;
static uint64_t *g_trade_ids;
static _Atomic int producer_done = 0;
static _Atomic int consumers_done = 0;
static int g_total_consumer_threads = 2;

struct thread_perf {
        struct timespec start;
        struct timespec end;
        unsigned long ops;
};

struct consumer_arg {
        struct thread_perf *perf;
        int idx;
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
                        int ret = trcache_feed_trade_data(&tc, &td, g_symbol_ids[s]);
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
                trcache_feed_trade_data(&tc, &td, g_symbol_ids[s]);
                perf->ops++;
        }
        perf_end(perf);
        atomic_store(&producer_done, 1);
        return NULL;
}

static void *consumer_tick_thread(void *arg)
{
        struct consumer_arg *carg = (struct consumer_arg *)arg;
        struct thread_perf *perf = carg->perf;

        int count = 0;
        for (int s = carg->idx; s < g_num_symbols; s += g_num_consumers)
                count++;

        struct trade_data_buffer_cursor **cursors =
                malloc(sizeof(*cursors) * count);
        int *syms = malloc(sizeof(*syms) * count);
        int j = 0;
        for (int s = carg->idx; s < g_num_symbols; s += g_num_consumers) {
                syms[j] = s;
                cursors[j] = trade_data_buffer_get_cursor(g_bufs[s],
                                TRCACHE_100TICK_CANDLE);
                j++;
        }
        perf_start(perf);

        while (1) {
                bool any = false;
                for (int i = 0; i < count; i++) {
                        int s = syms[i];
                        struct trcache_trade_data *array = NULL;
                        int n = 0;
                        if (trade_data_buffer_peek(g_bufs[s], cursors[i],
                                                   &array, &n)) {
                                for (int k = 0; k < n; k++)
                                        candle_chunk_list_apply_trade(
                                                g_list_ticks[s], &array[k]);
                                trade_data_buffer_consume(g_bufs[s], cursors[i],
                                                         n);
                                perf->ops += n;
                                any = true;
                        }
                }
                if (!any && atomic_load(&producer_done))
                        break;
        }

        perf_end(perf);
        free(cursors);
        free(syms);
        atomic_fetch_add(&consumers_done, 1);
        return NULL;
}

static void *consumer_sec_thread(void *arg)
{
        struct consumer_arg *carg = (struct consumer_arg *)arg;
        struct thread_perf *perf = carg->perf;

        int count = 0;
        for (int s = carg->idx; s < g_num_symbols; s += g_num_consumers)
                count++;

        struct trade_data_buffer_cursor **cursors =
                malloc(sizeof(*cursors) * count);
        int *syms = malloc(sizeof(*syms) * count);
        int j = 0;
        for (int s = carg->idx; s < g_num_symbols; s += g_num_consumers) {
                syms[j] = s;
                cursors[j] = trade_data_buffer_get_cursor(g_bufs[s],
                                TRCACHE_1SEC_CANDLE);
                j++;
        }
        perf_start(perf);

        while (1) {
                bool any = false;
                for (int i = 0; i < count; i++) {
                        int s = syms[i];
                        struct trcache_trade_data *array = NULL;
                        int n = 0;
                        if (trade_data_buffer_peek(g_bufs[s], cursors[i],
                                                   &array, &n)) {
                                for (int k = 0; k < n; k++)
                                        candle_chunk_list_apply_trade(
                                                g_list_secs[s], &array[k]);
                                trade_data_buffer_consume(g_bufs[s], cursors[i],
                                                         n);
                                perf->ops += n;
                                any = true;
                        }
                }
                if (!any && atomic_load(&producer_done))
                        break;
        }

        perf_end(perf);
        free(cursors);
        free(syms);
        atomic_fetch_add(&consumers_done, 1);
        return NULL;
}

static void *convert_thread(void *arg)
{
        struct thread_perf *perf = (struct thread_perf *)arg;
        perf_start(perf);

        while (atomic_load(&consumers_done) < g_total_consumer_threads) {
                for (int s = 0; s < g_num_symbols; s++) {
                        candle_chunk_list_convert_to_column_batch(g_list_ticks[s]);
                        candle_chunk_list_convert_to_column_batch(g_list_secs[s]);
                        perf->ops += 2;
                }
        }

        perf_end(perf);
        return NULL;
}

static void *flush_thread(void *arg)
{
        struct thread_perf *perf = (struct thread_perf *)arg;
        perf_start(perf);

        while (atomic_load(&consumers_done) < g_total_consumer_threads) {
                for (int s = 0; s < g_num_symbols; s++) {
                        candle_chunk_list_flush(g_list_ticks[s]);
                        candle_chunk_list_flush(g_list_secs[s]);
                        perf->ops += 2;
                }
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
        unsigned int seed = (unsigned int)time(NULL) ^ (uintptr_t)pthread_self();

        while (atomic_load(&consumers_done) < g_total_consumer_threads) {
                int idx = rand_r(&seed) % g_num_symbols;
                if (atomic_load(&g_list_ticks[idx]->mutable_seq) != UINT64_MAX &&
                    atomic_load(&g_list_ticks[idx]->mutable_seq) > 5 &&
                    trcache_get_candles_by_symbol_id_and_offset(
                                &tc, g_symbol_ids[idx],
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
        unsigned int seed = (unsigned int)time(NULL) ^ (uintptr_t)pthread_self();

        while (atomic_load(&consumers_done) < g_total_consumer_threads) {
                int idx = rand_r(&seed) % g_num_symbols;
                if (atomic_load(&g_list_secs[idx]->mutable_seq) != UINT64_MAX &&
                    atomic_load(&g_list_secs[idx]->mutable_seq) > 5) {
                        struct timespec now;
                        clock_gettime(CLOCK_REALTIME, &now);
                        uint64_t ms = now.tv_sec * 1000ULL + now.tv_nsec / 1000000ULL;
                        uint64_t ts = (ms / 1000ULL) * 1000ULL;

                        if (trcache_get_candles_by_symbol_id_and_ts(
                                        &tc, g_symbol_ids[idx],
                                        TRCACHE_1SEC_CANDLE, mask, ts, 5, batch) == 0) {
                                for (int i = 0; i < batch->num_candles; i++)
                                        check_sec_candle(batch, i);
                                perf->ops++;
                        } else {
                                sched_yield();
                        }
                } else {
                        sched_yield();
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
        if (argc > 2)
                g_num_consumers = atoi(argv[2]);
        if (argc > 3)
                g_num_symbols = atoi(argv[3]);

        g_total_consumer_threads = g_num_consumers * 2;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        g_end_time = now;
        g_end_time.tv_sec += g_total_seconds;

        pthread_key_create(&tc.pthread_trcache_key, NULL);
        pthread_mutex_init(&tc.tls_id_mutex, NULL);



        tc.candle_type_flags = TRCACHE_100TICK_CANDLE | TRCACHE_1SEC_CANDLE;
        tc.num_candle_types = 2;
        tc.batch_candle_count_pow2 = 10;
        tc.batch_candle_count = 1 << tc.batch_candle_count_pow2;
        tc.flush_threshold_pow2 = 1;
        tc.flush_threshold = 1 << tc.flush_threshold_pow2;
        tc.flush_ops.flush = dummy_flush;
        tc.flush_ops.is_done = dummy_is_done;
        tc.flush_ops.destroy_handle = dummy_destroy;

        tc.symbol_table = symbol_table_init(g_num_symbols * 2);
        assert(tc.symbol_table != NULL);
        int dummy_id = symbol_table_register(&tc, tc.symbol_table, "dummy");
        (void)dummy_id;
        g_symbol_ids = calloc(g_num_symbols, sizeof(int));
        g_syms = calloc(g_num_symbols, sizeof(*g_syms));
        g_bufs = calloc(g_num_symbols, sizeof(*g_bufs));
        g_list_ticks = calloc(g_num_symbols, sizeof(*g_list_ticks));
        g_list_secs = calloc(g_num_symbols, sizeof(*g_list_secs));
        g_trade_ids = calloc(g_num_symbols, sizeof(uint64_t));
        assert(g_symbol_ids && g_syms && g_bufs && g_list_ticks && g_list_secs && g_trade_ids);
        char name[16];
        for (int i = 0; i < g_num_symbols; i++) {
                snprintf(name, sizeof(name), "sym%d", i);
                g_symbol_ids[i] = symbol_table_register(&tc, tc.symbol_table, name);
                assert(g_symbol_ids[i] >= 0);
                g_syms[i] = symbol_table_lookup_entry(tc.symbol_table, g_symbol_ids[i]);
                assert(g_syms[i] != NULL);
                g_bufs[i] = g_syms[i]->trd_buf;
                g_list_ticks[i] = g_syms[i]->candle_chunk_list_ptrs[__builtin_ctz(TRCACHE_100TICK_CANDLE)];
                g_list_secs[i] = g_syms[i]->candle_chunk_list_ptrs[__builtin_ctz(TRCACHE_1SEC_CANDLE)];
                g_trade_ids[i] = 0;
        }

        pthread_t prod_t, conv_t, flush_t, read_t, read_sec_t;
        pthread_t *cons_tick_t = calloc(g_num_consumers, sizeof(pthread_t));
        pthread_t *cons_sec_t = calloc(g_num_consumers, sizeof(pthread_t));
        struct thread_perf prod_perf, conv_perf, flush_perf, read_perf, read_sec_perf;
        struct thread_perf *cons_tick_perf = calloc(g_num_consumers, sizeof(struct thread_perf));
        struct thread_perf *cons_sec_perf = calloc(g_num_consumers, sizeof(struct thread_perf));
        struct consumer_arg *cons_tick_args = calloc(g_num_consumers, sizeof(struct consumer_arg));
        struct consumer_arg *cons_sec_args = calloc(g_num_consumers, sizeof(struct consumer_arg));

        pthread_create(&prod_t, NULL, producer_thread, &prod_perf);
        for (int i = 0; i < g_num_consumers; i++) {
                cons_tick_args[i].perf = &cons_tick_perf[i];
                cons_tick_args[i].idx = i;
                pthread_create(&cons_tick_t[i], NULL, consumer_tick_thread,
                               &cons_tick_args[i]);
        }
        for (int i = 0; i < g_num_consumers; i++) {
                cons_sec_args[i].perf = &cons_sec_perf[i];
                cons_sec_args[i].idx = i;
                pthread_create(&cons_sec_t[i], NULL, consumer_sec_thread,
                               &cons_sec_args[i]);
        }
        pthread_create(&conv_t, NULL, convert_thread, &conv_perf);
        pthread_create(&flush_t, NULL, flush_thread, &flush_perf);
        pthread_create(&read_t, NULL, reader_tick_thread, &read_perf);
        pthread_create(&read_sec_t, NULL, reader_sec_thread, &read_sec_perf);

        pthread_join(prod_t, NULL);
        for (int i = 0; i < g_num_consumers; i++)
                pthread_join(cons_tick_t[i], NULL);
        for (int i = 0; i < g_num_consumers; i++)
                pthread_join(cons_sec_t[i], NULL);
        pthread_join(conv_t, NULL);
        pthread_join(flush_t, NULL);
        pthread_join(read_t, NULL);
        pthread_join(read_sec_t, NULL);

        symbol_table_destroy(tc.symbol_table);
        pthread_key_delete(tc.pthread_trcache_key);
        pthread_mutex_destroy(&tc.tls_id_mutex);
        free(g_symbol_ids);
        free(g_syms);
        free(g_bufs);
        free(g_list_ticks);
        free(g_list_secs);
        free(g_trade_ids);
        free(cons_tick_t);
        free(cons_sec_t);
        free(cons_tick_perf);
        free(cons_sec_perf);
        free(cons_tick_args);
        free(cons_sec_args);

        double prod_time = timespec_to_sec(&prod_perf.end) - timespec_to_sec(&prod_perf.start);
        double cons_tick_time = 0.0;
        double cons_sec_time = 0.0;
        unsigned long cons_tick_ops = 0;
        unsigned long cons_sec_ops = 0;
        for (int i = 0; i < g_num_consumers; i++) {
                cons_tick_time += timespec_to_sec(&cons_tick_perf[i].end) -
                                   timespec_to_sec(&cons_tick_perf[i].start);
                cons_tick_ops += cons_tick_perf[i].ops;
                cons_sec_time += timespec_to_sec(&cons_sec_perf[i].end) -
                                  timespec_to_sec(&cons_sec_perf[i].start);
                cons_sec_ops += cons_sec_perf[i].ops;
        }
        double conv_time = timespec_to_sec(&conv_perf.end) - timespec_to_sec(&conv_perf.start);
        double flush_time = timespec_to_sec(&flush_perf.end) - timespec_to_sec(&flush_perf.start);
        double read_time = timespec_to_sec(&read_perf.end) - timespec_to_sec(&read_perf.start);
        double read_sec_time = timespec_to_sec(&read_sec_perf.end) - timespec_to_sec(&read_sec_perf.start);

        printf("Producer: %.2f ops/sec\n", prod_perf.ops / prod_time);
        printf("Consumer(tick): %.2f ops/sec\n", cons_tick_ops / cons_tick_time);
        printf("Consumer(sec): %.2f ops/sec\n", cons_sec_ops / cons_sec_time);
        printf("Converter loops: %.2f/sec\n", conv_perf.ops / conv_time);
        printf("Flusher loops: %.2f/sec\n", flush_perf.ops / flush_time);
        printf("Reader(tick) loops: %.2f/sec\n", read_perf.ops / read_time);
        printf("Reader(sec) loops: %.2f/sec\n", read_sec_perf.ops / read_sec_time);

        return 0;
}

