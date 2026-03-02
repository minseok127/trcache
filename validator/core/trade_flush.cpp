/*
 * validator/core/trade_flush.cpp
 *
 * Simple synchronous pwrite implementation of trcache_trade_flush_ops.
 *
 * Thread-safety model
 * -------------------
 * trcache guarantees that each symbol is owned by at most one flush worker
 * at any point in time (enforced by a CAS on trade_flush_ownership_flags).
 * Therefore ctx->fds[symbol_id] and ctx->write_offsets[symbol_id] are
 * accessed by at most one thread at a time; no extra lock is needed.
 *
 * Error handling
 * --------------
 * Write failures are printed to stderr and the chunk is treated as done
 * so the pipeline keeps running.
 */

#include "trade_flush.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <iostream>

/* --------------------------------------------------------------------------
 * Internal structures
 * -------------------------------------------------------------------------- */

struct trade_flush_ctx {
	int               *fds;           /* fds[symbol_id], -1 = not yet opened */
	/* write_offsets[symbol_id], next write position */
	size_t            *write_offsets;
	int                max_symbols;
	size_t             trade_data_size;
	/* PATH_MAX/2 leaves room for "/<symbol>" in the per-file path buffer. */
	char               output_dir[PATH_MAX / 2];
};

/* --------------------------------------------------------------------------
 * Callback implementations
 * -------------------------------------------------------------------------- */

static void flush_cb(struct trcache *cache, int symbol_id,
		const void *io_block, int num_trades,
		void *ctx_ptr)
{
	struct trade_flush_ctx *ctx = (struct trade_flush_ctx *)ctx_ptr;

	/* Lazy-open: only this worker touches fds[symbol_id] (CAS guarantee). */
	if (ctx->fds[symbol_id] < 0) {
		const char *sym = trcache_lookup_symbol_str(cache, symbol_id);
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s/%s", ctx->output_dir, sym);

		ctx->fds[symbol_id] = open(path, O_WRONLY | O_CREAT, 0644);
		if (ctx->fds[symbol_id] < 0) {
			std::cerr << "[TradeFlush] open failed: sym=" << sym
				  << " err=" << strerror(errno) << "\n";
			return;
		}
	}

	int fd = ctx->fds[symbol_id];
	size_t write_size = (size_t)num_trades * ctx->trade_data_size;
	size_t offset = ctx->write_offsets[symbol_id];
	ctx->write_offsets[symbol_id] += write_size;

	ssize_t n = pwrite(fd, io_block, write_size, (off_t)offset);
	if (n < 0 || (size_t)n != write_size) {
		const char *sym = trcache_lookup_symbol_str(cache, symbol_id);
		std::cerr << "[TradeFlush] pwrite failed: sym=" << sym
			  << " offset=" << offset
			  << " trades=" << num_trades
			  << " err=" << strerror(errno) << "\n";
	}
}

/*
 * is_done_cb - pwrite is synchronous; flush is always complete immediately.
 */
static bool is_done_cb(struct trcache * /*cache*/,
		const void * /*io_block*/, void * /*ctx_ptr*/)
{
	return true;
}

/* --------------------------------------------------------------------------
 * Public API
 * -------------------------------------------------------------------------- */

struct trade_flush_ctx *trade_flush_ctx_create(int max_symbols,
	size_t trade_data_size, const char *output_dir)
{
	struct trade_flush_ctx *ctx = new (std::nothrow) trade_flush_ctx();
	if (!ctx)
		return NULL;

	ctx->fds = (int *)malloc((size_t)max_symbols * sizeof(int));
	if (!ctx->fds) {
		delete ctx;
		return NULL;
	}
	for (int i = 0; i < max_symbols; i++)
		ctx->fds[i] = -1;

	ctx->write_offsets = (size_t *)calloc((size_t)max_symbols, sizeof(size_t));
	if (!ctx->write_offsets) {
		free(ctx->fds);
		delete ctx;
		return NULL;
	}

	ctx->max_symbols     = max_symbols;
	ctx->trade_data_size = trade_data_size;
	snprintf(ctx->output_dir, sizeof(ctx->output_dir) - 1, "%s", output_dir);
	ctx->output_dir[sizeof(ctx->output_dir) - 1] = '\0';

	if (mkdir(output_dir, 0755) < 0 && errno != EEXIST) {
		std::cerr << "[TradeFlush] mkdir failed for " << output_dir
			  << ": " << strerror(errno) << "\n";
		/* Continue; individual open() calls will surface real errors. */
	}

	return ctx;
}

void trade_flush_ctx_destroy(struct trade_flush_ctx *ctx)
{
	if (!ctx)
		return;

	for (int i = 0; i < ctx->max_symbols; i++) {
		if (ctx->fds[i] >= 0) {
			close(ctx->fds[i]);
			ctx->fds[i] = -1;
		}
	}

	free(ctx->write_offsets);
	free(ctx->fds);
	delete ctx;
}

struct trcache_trade_flush_ops trade_flush_get_ops(struct trade_flush_ctx *ctx)
{
	struct trcache_trade_flush_ops ops = {};
	ops.flush   = flush_cb;
	ops.is_done = is_done_cb;
	ops.ctx     = ctx;
	return ops;
}
