/*
 * @file   candle_chunk.c
 * @brief  Implementation of row-oriented -> column-oriented staging.
 *
 * This module manages one candle_chunk, which buffers real-time trades into
 * row-oriented pages, converts them to column-oriented batches, and finally
 * flushes the result.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>

#include "core/candle_chunk.h"
#include "core/trcache_internal.h"
#include "utils/log.h"

/**
 * @brief   Allocate an atomsnap_version and its owned candle row page.
 *
 * The returned version already contains a zero‑initialized, 64‑byte‑aligned
 * row page in its @object field.
 *
 * @return  Pointer to version, or NULL on failure.
 */
static struct atomsnap_version *row_page_version_alloc(
	void *unused __attribute__((unused)))
{
	struct atomsnap_version *version  = NULL;
	struct candle_row_page *row_page = NULL;

#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	row_page = aligned_alloc(TRCACHE_SIMD_ALIGN,
		sizeof(struct candle_row_page));
#else
	posix_memalign((void **)&row_page, TRCACHE_SIMD_ALIGN,
		sizeof(struct candle_row_page));
#endif

	if (!row_page) {
		errmsg(stderr, "#candle_row_page allocation failedn\n");
		return NULL;
	} else {
		memset(row_page, 0, sizeof(struct candle_row_page));
	}

	version = malloc(sizeof(struct atomsnap_version));
	if (version == NULL) {
		errmsg(stderr, "#atomsnap_version allocation failed\n");
		free(row_page);
		return NULL;
	}

	version->object = row_page;
	return version;
}

/**
 * @brief   Final cleanup for a row page version.
 *
 * @param   version: Pointer to the atomsnap_version
 *
 * Called by the last thread to release its reference to the version.
 */
static void row_page_version_free(struct atomsnap_version *version)
{
	if (version == NULL) {
		return;
	}

	free(version->object); /* #candle_row_page */
	free(version);
}

/**
 * @brief   Allocate and initialize #candle_chunk.
 *
 * @param   candle_type:        Candle type of the column-batch.
 * @param   symbol_id:          Symbol ID of the column-batch.
 * @param   row_page_count:     Number of row pages per chunk.
 * @param   batch_candle_count: Number of candles per chunk.   
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
struct candle_chunk *create_candle_chunk(trcache_candle_type candle_type,
	int symbol_id, int row_page_count, int batch_candle_count)
{
	struct candle_chunk *chunk = NULL;
	struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = row_page_version_alloc,
		.atomsnap_free_impl = row_page_version_free,
		.num_extra_control_blocks = row_page_count - 1
	};

	chunk = malloc(sizeof(struct candle_chunk));
	if (chunk == NULL) {
		errmsg(stderr, "#candle_chunk allocation failed\n");
		return NULL;
	}

	if (pthread_spin_init(&chunk->spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
		errmsg(stderr, "Initialization of spinlock failed\n");
		free(chunk);
		return NULL;
	}

	chunk->row_gate = atomsnap_init_gate(&ctx);
	if (chunk->row_gate == NULL) {
		errmsg(stderr, "Failure on atomsnap_init_gate\n");
		pthread_spin_destroy(&chunk->spinlock);
		free(chunk);
		return NULL;
	}

	chunk->column_batch = trcache_batch_alloc_on_heap(batch_candle_count);
	if (chunk->column_batch == NULL) {
		errmsg(stderr, "Failure on trcache_batch_alloc_on_head()\n");
		pthread_spin_destroy(&chunk->spinlock);
		atomsnap_destroy_gate(chunk->row_gate);
		free(chunk);
		return NULL;
	}

	chunk->column_batch->symbol_id = symbol_id;
	chunk->column_batch->candle_type = candle_type;

	chunk->mutable_page_idx = -1;
	chunk->mutable_row_idx = -1;
	chunk->converting_page_idx = 0;
	chunk->converting_row_idx = 0;
	chunk->is_flushed = 0;
	chunk->flush_handle = NULL;
	chunk->seq_first = UINT64_MAX;

	atomic_store(&chunk->num_completed, 0);
	atomic_store(&chunk->num_converted, 0);

	chunk->next = NULL;
	chunk->prev = NULL;

	return chunk;
}

/**
 * @brief   Release all resources of a candle chunk.
 *
 * @param   chunk: Candle-chunk pointer.
 */
void candle_chunk_destroy(struct candle_chunk *chunk)
{
	if (chunk == NULL) {
		return;
	}

	pthread_spin_destroy(&chunk->spinlock);
	atomsnap_destroy_gate(chunk->row_gate);
	trcache_batch_free(chunk->column_batch);
	free(chunk);
}

/**
 * @brief   Initialize a row page within a candle chunk.
 *
 * @param   chunk:     Pointer to the candle chunk.
 * @param   page_idx:  Index of the page to initialize.
 * @param   ops:       Callback operations for candle initialization.
 * @param   trade:     First trade data used to initialize the first candle.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_page_init(struct candle_chunk *chunk, int page_idx,
	struct candle_update_ops *ops, struct trcache_trade_data *trade)
{
	struct candle_row_page *row_page = NULL;
	struct atomsnap_version *row_page_version
		= atomsnap_make_version(chunk->row_gate, NULL);

	if (row_page_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		return -1;
	}

	/*
	 * Since the completion count has not been incremented yet, the candle
	 * initialization process is not visible to readers. Therefore, no locking
	 * is required.
	 */
	row_page = (struct candle_row_page *)row_page_version->object;
	ops->init(&(row_page->rows[0]), trade);
	atomsnap_exchange_version_slot(chunk->row_gate, page_idx, row_page_version);
	return 0;
}

/**
 * @brief   Convert all immutable row candles within the given chunk.
 *
 * @param   chunk:     Target chunk to convert.
 * @param   start_idx: Start record index to convert.
 * @param   end_idx:   End record index to convert.
 */
void candle_chunk_convert_to_batch(struct candle_chunk *chunk,
	int start_idx, int end_idx)
{
	struct trcache_candle_batch *batch = chunk->column_batch;
	int cur_page_idx = chunk->converting_page_idx, next_page_idx;
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, cur_page_idx);
	struct candle_row_page *page
		= (struct candle_row_page *)page_version->object;
	struct trcache_candle *c = NULL;

	/* Vector pointers */
	uint64_t *ts_ptr = batch->start_timestamp_array + start_idx;
	uint64_t *id_ptr = batch->start_trade_id_array + start_idx;
	double *o_ptr = batch->open_array + start_idx;
	double *h_ptr = batch->high_array + start_idx;
	double *l_ptr = batch->low_array + start_idx;
	double *c_ptr = batch->close_array + start_idx;
	double *v_ptr = batch->volume_array + start_idx;

	for (int idx = start_idx; idx <= end_idx; idx++) {
		next_page_idx = candle_chunk_calc_page_idx(idx);
		if (next_page_idx != cur_page_idx) {
			/*
			 * Page is fully converted. Tigger the grace period.
			 */
			atomsnap_exchange_version_slot(
				chunk->row_gate, cur_page_idx, NULL);
			atomsnap_release_version(page_version);

			cur_page_idx = next_page_idx;
			page_version = atomsnap_acquire_version_slot(
				chunk->row_gate, cur_page_idx);
			page = (struct candle_row_page *)page_version->object;
		}

		c = &(page->rows[candle_chunk_calc_row_idx(idx)]);

		*ts_ptr++ = c->start_timestamp;
		*id_ptr++ = c->start_trade_id;
		*o_ptr++ = c->open;
		*h_ptr++ = c->high;
		*l_ptr++ = c->low;
		*c_ptr++ = c->close;
		*v_ptr++ = c->volume;
	}
	atomsnap_release_version(page_version);

	/* This value is equal to chunk->num_completed */
	end_idx += 1;

	/* Remember converting context for this chunk */
	chunk->converting_page_idx = candle_chunk_calc_page_idx(end_idx);
	chunk->converting_row_idx = candle_chunk_calc_row_idx(end_idx);

	/* chunk->num_converted = chunk->num_completed */
	atomic_store_explicit(&chunk->num_converted, end_idx,
		memory_order_release);
}

/**
 * @brief   Flush a single fully-converted candle chunk.
 *
 * Starts a backend-specific flush on @chunk using the callbacks in
 * @trc->flush_ops. If the backend returns a non-NULL handle, the flush is
 * assumed to be asynchronous and remains "in-flight"; the caller must poll
 * it later with flush_ops->is_done(). When the backend performs a
 * synchronous flush it returns NULL, in which case the chunk is marked
 * immediately as flushed.
 *
 * @param   trc:       Pointer to the parent trcache instance.
 * @param   chunk:     Pointer to the target candle_chunk.
 *
 * @return  1  flush completed synchronously  
 *          0  flush started asynchronously (still pending)  
 */
int candle_chunk_flush(struct trcache *trc, struct candle_chunk *chunk)
{
	struct trcache_flush_ops *flush_ops = &trc->flush_ops;

	chunk->flush_handle = flush_ops->flush(
		trc, chunk->column_batch, flush_ops->flush_ctx);

	/* Synchronous flush - the backend finished right away. */
	if (chunk->flush_handle == NULL) {
		chunk->is_flushed = 1;
		return 1;
	}

	/* Asynchronous flush has begun. Caller must poll later. */
	chunk->is_flushed = 0;
	return 0;
}

/**
 * @brief   Poll a candle chunk for flush completion.
 *
 * If the chunk was flushed synchronously (@chunk->is_flushed == 1) the
 * function returns immediately. Otherwise it queries the backend via
 * flush_ops->is_done(). When the backend signals completion, the flush
 * handle is destroyed and the chunk is marked flushed.
 *
 * @param   trc:    Pointer to the parent trcache instance.
 * @param   chunk:  Pointer to the target candle_chunk.
 *
 * @return  1  flush has completed *in this call*.
 *          0  flush has not completed *in this call*.
 */
int candle_chunk_flush_poll(struct trcache *trc, struct candle_chunk *chunk)
{
	struct trcache_flush_ops *flush_ops = &trc->flush_ops;

	/* Synchronous flush already finished earlier. */
	if (chunk->is_flushed) {
		return 0;
	}

	/* Ask the backend whether the I/O has finished. */
	if (flush_ops->is_done(trc, chunk->column_batch, chunk->flush_handle)) {
		/* Tear down backend resources and mark completion. */
		flush_ops->destroy_handle(chunk->flush_handle,
			flush_ops->destroy_handle_ctx);
		chunk->flush_handle = NULL;
		chunk->is_flushed = 1;
		return 1;
	}

	/* Still pending. */
	return 0;
}

_Static_assert(TRCACHE_NUM_CANDLE_FIELD == 7, "Field count/dispatch mismatch");

/**
 * @brief   Dispatch-table based copier (GNU computed-goto).
 *
 * The function jumps directly to the code block that handles the first set-bit
 * in @mask, copies one field, clears that bit, and jumps again until all
 * requested fields are processed. It never executes an indirect *call*,
 * only indirect *jumps*, so there is no call/return overhead.
 *
 * @param   c:     Pointer to the source candle (AoS).
 * @param   d:     Pointer to the destination batch (SoA).
 * @param   i:     Destination index to fill.
 * @param   mask:  Bit-mask describing which fields to copy (>= 1).
 *
 * @note  Uses GNU C's "computed goto". Portable only with GCC/Clang.
 *        The lowest bit (0) corresponds to START_TIMESTAMP.
 *        Undefined behaviour is avoided by checking @mask==0
 *        before every computed-goto re-entry.
 */
static inline void copy_dispatch_tbl(struct trcache_candle * __restrict c,
	struct trcache_candle_batch * __restrict d,
	int i, trcache_candle_field_flags mask)
{
	static void *dispatch[] = {
		&&copy_start_ts,
		&&copy_start_id,
		&&copy_open,
		&&copy_high,
		&&copy_low,
		&&copy_close,
		&&copy_volume,
		&&out,
	};

	uint32_t m = mask;
	if (m == 0) {
		return; /* nothing to copy */
	}

	/* Set the out bit */
	m |= (1 << TRCACHE_NUM_CANDLE_FIELD);
	goto *dispatch[__builtin_ctz(m)];

copy_start_ts:
	d->start_timestamp_array[i] = c->start_timestamp;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_start_id:
	d->start_trade_id_array[i] = c->start_trade_id;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_open:
	d->open_array[i] = c->open;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_high:
	d->high_array[i] = c->high;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_low:
	d->low_array[i] = c->low;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_close:
	d->close_array[i] = c->close;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_volume:
	d->volume_array[i] = c->volume;
	/* Fall through */

out:
	return;
}

/**
 * @brief   Copy a single mutable candle from a row page into a SoA batch.
 *
 * The function acquires the chunk's spin-lock, verifies that the target
 * record is still resident in a row page (i.e., has not yet been moved to
 * the column batch by the background *convert* thread), and copies the
 * requested fields into @dst.
 *
 * @param   chunk:       Pointer to the candle_chunk.
 * @param   record_idx:  Index of the record to copy (0-based).
 * @param   dst_idx:     Index of the last element in @dst to fill.
 * @param   dst [out]:   Pre-allocated destination batch.
 * @param   field_mask:  Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_mutable_row(struct candle_chunk *chunk,
	int record_idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask)
{
	int page_idx = candle_chunk_calc_page_idx(record_idx);
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, page_idx);
	struct candle_row_page *row_page;
	struct trcache_candle *candle;
	int row_idx;

	if (page_version == NULL) {
		return 0;
	}

	row_page = (struct candle_row_page *)page_version->object;
	row_idx = candle_chunk_calc_row_idx(record_idx);
	candle = __builtin_assume_aligned(&row_page->rows[row_idx], 64);

	pthread_spin_lock(&chunk->spinlock);
	copy_dispatch_tbl(candle, dst, dst_idx, field_mask);
	pthread_spin_unlock(&chunk->spinlock);

	atomsnap_release_version(page_version);
	return 1;
}

/**
 * @brief   Copy a contiguous range of row-oriented candles.
 *
 * Starting at @end_record_idx and walks towards @start_record_idx (inclusive),
 * this routine copies all rows that are still in row pages into @dst.
 * If the background convert thread converts the range midway, the
 * function stops early and returns the count copied so far.
 *
 * @param   chunk:             Pointer to the candle_chunk.
 * @param   start_record_idx:  First record index in the range (inclusive).
 * @param   end_record_idx:    Last  record index in the range (inclusive).
 * @param   dst_idx:           Index of the last element in @dst to fill.
 * @param   dst [out]:         Pre-allocated destination batch.
 * @param   field_mask:        Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_rows_until_converted(struct candle_chunk *chunk,
	int start_record_idx, int end_record_idx, int dst_idx,
	struct trcache_candle_batch *dst, trcache_candle_field_flags field_mask)
{
	int cur_page_idx = candle_chunk_calc_page_idx(end_record_idx);
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, cur_page_idx);
	struct candle_row_page *row_page;
	struct trcache_candle *candle;
	int next_page_idx, row_idx, num_copied = 0;

	if (page_version == NULL) {
		return 0;
	}

	row_page = (struct candle_row_page *)page_version->object;

	for (int idx = end_record_idx; idx >= start_record_idx; idx--) {
		next_page_idx = candle_chunk_calc_page_idx(idx);
		if (next_page_idx != cur_page_idx) {
			atomsnap_release_version(page_version);

			cur_page_idx = next_page_idx;
			page_version = atomsnap_acquire_version_slot(
				chunk->row_gate, cur_page_idx);
			if (page_version == NULL) {
				break;
			}

			row_page = (struct candle_row_page *)page_version->object;
		}

		row_idx = candle_chunk_calc_row_idx(idx);
		candle = __builtin_assume_aligned(&row_page->rows[row_idx], 64);
		copy_dispatch_tbl(candle, dst, dst_idx, field_mask);

		num_copied += 1;
		dst_idx -= 1;
	}

	return num_copied;
}

/**
 * @brief   Copy candles that already reside in the column batch.
 *
 * Unlike the previous helpers, this routine bypasses row pages entirely and
 * pulls data from the chunk-local columnar storage area. It therefore
 * requires that the specified record range has *already* been converted.
 *
 * @param   chunk:              Pointer to the candle_chunk.
 * @param   start_record_idx:   First record index in the range (inclusive).
 * @param   end_record_idx:     Last  record index in the range (inclusive).
 * @param   dst_idx:            Index of the last element in @dst to fill.
 * @param   dst [out]:          Pre-allocated destination batch.
 * @param   field_mask:         Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_from_column_batch(struct candle_chunk *chunk,
	int start_record_idx, int end_record_idx, int dst_idx,
	struct trcache_candle_batch *dst, trcache_candle_field_flags field_mask)
{

}
