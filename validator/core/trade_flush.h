/*
 * validator/core/trade_flush.h
 *
 * Synchronous pwrite-based raw trade chunk flush for the validator.
 *
 * Each symbol's trade data is appended to a file named after the symbol
 * string (e.g. "BTCUSDT") under a configurable output directory.
 *
 * Per-symbol file descriptors are opened lazily on first flush and are
 * only ever touched by the one worker that holds the symbol's CAS
 * ownership flag, so no additional synchronization is needed.
 */

#ifndef VALIDATOR_CORE_TRADE_FLUSH_H
#define VALIDATOR_CORE_TRADE_FLUSH_H

#include "trcache.h"

struct trade_flush_ctx;

/*
 * trade_flush_ctx_create - Allocate and initialize a trade flush context.
 *
 * @max_symbols:      Upper bound on symbol IDs (passed directly as
 *                    trcache_init_ctx::max_symbols).
 * @trade_data_size:  Size in bytes of one trade record.
 * @output_dir:       Directory under which per-symbol files are created.
 *                    Created automatically if it does not already exist.
 *
 * Returns a pointer to the context, or NULL on failure.
 */
struct trade_flush_ctx *trade_flush_ctx_create(int max_symbols,
	size_t trade_data_size, const char *output_dir);

/*
 * trade_flush_ctx_destroy - Close all open file descriptors and free
 * the context.
 */
void trade_flush_ctx_destroy(struct trade_flush_ctx *ctx);

/*
 * trade_flush_get_ops - Return a trcache_trade_flush_ops struct wired up
 * to the pwrite callbacks.  Pass the result directly to
 * trcache_init_ctx::trade_flush_ops.
 *
 * Write failures are printed to stderr; the failed chunk is treated as done
 * so the pipeline keeps running.
 */
struct trcache_trade_flush_ops trade_flush_get_ops(struct trade_flush_ctx *ctx);

#endif /* VALIDATOR_CORE_TRADE_FLUSH_H */
