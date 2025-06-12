#include <assert.h>
#include "trcache.h"

static void *dummy_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
	(void)cache; (void)batch; (void)ctx; return NULL;
}
static bool dummy_is_done(trcache *cache, trcache_candle_batch *batch, void *h) {
	(void)cache; (void)batch; (void)h; return true;
}
static void dummy_destroy(void *h, void *ctx) { (void)h; (void)ctx; }

int main(void) {
	struct trcache_init_ctx ctx = {
	    .num_worker_threads = 2,
	    .batch_candle_count_pow2 = 2,
	    .flush_threshold_pow2 = 1,
	    .candle_type_flags = TRCACHE_1MIN_CANDLE,
	    .flush_ops = {
		.flush = dummy_flush,
		.is_done = dummy_is_done,
		.destroy_handle = dummy_destroy,
		.flush_ctx = NULL,
		.destroy_handle_ctx = NULL
	    }
	};

	trcache *cache = trcache_init(&ctx);
	assert(cache != NULL);

	trcache_destroy(cache);
	return 0;
}
