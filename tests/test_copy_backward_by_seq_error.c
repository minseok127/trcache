#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include "meta/trcache_internal.h"
#include "pipeline/candle_chunk_list.h"
#include "pipeline/candle_update_ops.h"

static void *dummy_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
    (void)cache; (void)batch; (void)ctx; return NULL;
}
static bool dummy_is_done(trcache *cache, trcache_candle_batch *batch, void *h) {
    (void)cache; (void)batch; (void)h; return true;
}
static void dummy_destroy(void *h, void *ctx) { (void)h; (void)ctx; }

int main(void) {
    struct trcache tc = {0};
    tc.candle_type_flags = TRCACHE_100TICK_CANDLE;
    tc.num_candle_types = 1;
    tc.batch_candle_count_pow2 = 2;
    tc.batch_candle_count = 1 << tc.batch_candle_count_pow2;
    tc.flush_threshold_pow2 = 1;
    tc.flush_threshold = 1 << tc.flush_threshold_pow2;
    tc.flush_ops.flush = dummy_flush;
    tc.flush_ops.is_done = dummy_is_done;
    tc.flush_ops.destroy_handle = dummy_destroy;

    struct candle_chunk_list_init_ctx ctx = {
        .trc = &tc,
        .update_ops = get_candle_update_ops(TRCACHE_100TICK_CANDLE),
        .candle_type = TRCACHE_100TICK_CANDLE,
        .symbol_id = 0
    };
    struct candle_chunk_list *list = create_candle_chunk_list(&ctx);
    assert(list != NULL);

    struct trcache_trade_data td = {0};
    td.timestamp = 0;
    td.trade_id = 0;
    td.price = 1.0;
    td.volume = 1.0;

    /* Initialize the first candle */
    assert(candle_chunk_list_apply_trade(list, &td) == 0);

    struct trcache_candle_batch *batch =
        trcache_batch_alloc_on_heap(1, TRCACHE_FIELD_MASK_ALL);
    assert(batch != NULL);

    /* Request a sequence that is not in the index */
    int ret = candle_chunk_list_copy_backward_by_seq(list, 10, 1, batch,
                TRCACHE_FIELD_MASK_ALL);
    assert(ret == -1);

    trcache_batch_free(batch);
    destroy_candle_chunk_list(list);
    return 0;
}
