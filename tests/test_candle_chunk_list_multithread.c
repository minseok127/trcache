#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

#include "meta/trcache_internal.h"
#include "pipeline/candle_chunk_list.h"
#include "pipeline/trade_data_buffer.h"

#define TRADES_PER_CANDLE 100
#define NUM_CANDLES 100
#define NUM_TRADES (TRADES_PER_CANDLE * NUM_CANDLES)

static struct trade_data_buffer *g_buf;
static struct candle_chunk_list *g_list;
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
    for (int i = 0; i < NUM_TRADES; i++) {
        struct trcache_trade_data td = {
            .timestamp = i,
            .trade_id = i,
            .price = (double)i,
            .volume = 1.0
        };
        int ret = trade_data_buffer_push(g_buf, &td, NULL);
        assert(ret == 0);
        perf->ops++;
    }
    /* Extra trade to close the last candle */
    struct trcache_trade_data td = {
        .timestamp = NUM_TRADES,
        .trade_id = NUM_TRADES,
        .price = (double)NUM_TRADES,
        .volume = 1.0
    };
    trade_data_buffer_push(g_buf, &td, NULL);
    perf->ops++;
    perf_end(perf);
    atomic_store(&producer_done, 1);
    return NULL;
}

static void *consumer_thread(void *arg)
{
    struct thread_perf *perf = (struct thread_perf *)arg;
    struct trade_data_buffer_cursor *cursor =
        trade_data_buffer_get_cursor(g_buf, TRCACHE_100TICK_CANDLE);
    int consumed = 0;
    perf_start(perf);
    while (consumed < NUM_TRADES + 1) {
        struct trcache_trade_data *array = NULL;
        int count = 0;
        int has = trade_data_buffer_peek(g_buf, cursor, &array, &count);
        if (has && count > 0) {
            for (int i = 0; i < count; i++) {
                int ret = candle_chunk_list_apply_trade(g_list, &array[i]);
                assert(ret == 0);
            }
            trade_data_buffer_consume(g_buf, cursor, count);
            consumed += count;
            perf->ops += count;
        } else {
            if (atomic_load(&producer_done) && consumed >= NUM_TRADES + 1)
                break;
            sched_yield();
        }
    }
    perf_end(perf);
    return NULL;
}

static void *convert_thread(void *arg)
{
    struct thread_perf *perf = (struct thread_perf *)arg;
    uint64_t target_last = NUM_CANDLES - 1; /* last completed candle */
    perf_start(perf);
    while (atomic_load(&g_list->last_seq_converted) < target_last) {
        candle_chunk_list_convert_to_column_batch(g_list);
        perf->ops++;
        sched_yield();
    }
    perf_end(perf);
    return NULL;
}

static void *flush_thread(void *arg)
{
    struct thread_perf *perf = (struct thread_perf *)arg;
    uint64_t target_last = NUM_CANDLES - 1;
    perf_start(perf);
    while (true) {
        candle_chunk_list_flush(g_list);
        perf->ops++;
        if (atomic_load(&g_list->last_seq_converted) >= target_last &&
            atomic_load(&g_list->unflushed_batch_count) == 0 &&
            atomic_load(&producer_done))
            break;
        sched_yield();
    }
    perf_end(perf);
    return NULL;
}

static void check_candle(const struct trcache_candle_batch *b, int idx)
{
    uint64_t base = b->start_trade_id_array[idx];
    assert(base % TRADES_PER_CANDLE == 0);
    assert(b->open_array[idx] == (double)base);
    assert(b->close_array[idx] == (double)(base + TRADES_PER_CANDLE - 1));
    assert(b->high_array[idx] == b->close_array[idx]);
    assert(b->low_array[idx] == b->open_array[idx]);
    assert(b->volume_array[idx] == (double)TRADES_PER_CANDLE);
}

static void *reader_thread(void *arg)
{
    struct thread_perf *perf = (struct thread_perf *)arg;
    struct trcache_candle_batch *batch = trcache_batch_alloc_on_heap(5);
    trcache_candle_field_flags mask = TRCACHE_START_TRADE_ID | TRCACHE_OPEN |
        TRCACHE_HIGH | TRCACHE_LOW | TRCACHE_CLOSE | TRCACHE_VOLUME;
    perf_start(perf);
    while (true) {
        uint64_t last = atomic_load(&g_list->last_seq_converted);
        if (last >= 4) {
            int ret = candle_chunk_list_copy_backward_by_seq(g_list, last, 5,
                    batch, mask);
            if (ret == 0) {
                for (int i = 0; i < batch->num_candles; i++) {
                    check_candle(batch, i);
                }
                perf->ops++;
            }
        }
        if (atomic_load(&producer_done) &&
            last >= NUM_CANDLES - 1 &&
            atomic_load(&g_list->unflushed_batch_count) == 0)
            break;
        sched_yield();
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

int main(void)
{
    struct trcache tc = {0};
    tc.candle_type_flags = TRCACHE_100TICK_CANDLE;
    tc.num_candle_types = 1;
    tc.batch_candle_count_pow2 = 3; /* 8 candles per chunk */
    tc.batch_candle_count = 1 << tc.batch_candle_count_pow2;
    tc.flush_threshold_pow2 = 1; /* flush after 2 batches */
    tc.flush_threshold = 1 << tc.flush_threshold_pow2;
    tc.flush_ops.flush = dummy_flush;
    tc.flush_ops.is_done = dummy_is_done;
    tc.flush_ops.destroy_handle = dummy_destroy;

    g_buf = trade_data_buffer_init(&tc);
    assert(g_buf != NULL);

    struct candle_chunk_list_init_ctx ctx = {
        .trc = &tc,
        .update_ops = get_candle_update_ops(TRCACHE_100TICK_CANDLE),
        .candle_type = TRCACHE_100TICK_CANDLE,
        .symbol_id = 0
    };
    g_list = create_candle_chunk_list(&ctx);
    assert(g_list != NULL);

    pthread_t prod_t, cons_t, conv_t, flush_t, read_t;
    struct thread_perf prod_perf, cons_perf, conv_perf, flush_perf, read_perf;

    pthread_create(&prod_t, NULL, producer_thread, &prod_perf);
    pthread_create(&cons_t, NULL, consumer_thread, &cons_perf);
    pthread_create(&conv_t, NULL, convert_thread, &conv_perf);
    pthread_create(&flush_t, NULL, flush_thread, &flush_perf);
    pthread_create(&read_t, NULL, reader_thread, &read_perf);

    pthread_join(prod_t, NULL);
    pthread_join(cons_t, NULL);
    pthread_join(conv_t, NULL);
    pthread_join(flush_t, NULL);
    pthread_join(read_t, NULL);

    destroy_candle_chunk_list(g_list);
    trade_data_buffer_destroy(g_buf);

    double prod_time = timespec_to_sec(&prod_perf.end) - timespec_to_sec(&prod_perf.start);
    double cons_time = timespec_to_sec(&cons_perf.end) - timespec_to_sec(&cons_perf.start);
    double conv_time = timespec_to_sec(&conv_perf.end) - timespec_to_sec(&conv_perf.start);
    double flush_time = timespec_to_sec(&flush_perf.end) - timespec_to_sec(&flush_perf.start);
    double read_time = timespec_to_sec(&read_perf.end) - timespec_to_sec(&read_perf.start);

    printf("Producer: %.2f ops/sec\n", prod_perf.ops / prod_time);
    printf("Consumer: %.2f ops/sec\n", cons_perf.ops / cons_time);
    printf("Converter loops: %.2f/sec\n", conv_perf.ops / conv_time);
    printf("Flusher loops: %.2f/sec\n", flush_perf.ops / flush_time);
    printf("Reader loops: %.2f/sec\n", read_perf.ops / read_time);

    return 0;
}

