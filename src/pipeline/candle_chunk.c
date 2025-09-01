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

#include "meta/trcache_internal.h"
#include "pipeline/candle_chunk.h"
#include "utils/log.h"

/**
 * @brief   Allocate an atomsnap_version and its owned candle row page.
 *
 * @param   mem_acc_arg: Memory accounting information.
 *
 * The returned version already contains a zero‑initialized, 64‑byte‑aligned
 * row page in its @object field.
 *
 * @return  Pointer to version, or NULL on failure.
 */
static struct atomsnap_version *row_page_version_alloc(void *mem_acc_arg)
{
	struct atomsnap_version *version  = NULL;
	struct candle_row_page *row_page = NULL;
	struct memory_accounting *mem_acc = (struct memory_accounting *)mem_acc_arg;

#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	row_page = aligned_alloc(TRCACHE_SIMD_ALIGN,
		sizeof(struct candle_row_page));
#else
	posix_memalign((void **)&row_page, TRCACHE_SIMD_ALIGN,
		sizeof(struct candle_row_page));
#endif

	if (!row_page) {
		errmsg(stderr, "#candle_row_page allocation failed\n");
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

	memstat_add(&mem_acc->ms, MEMSTAT_CANDLE_CHUNK_LIST,
		 sizeof(struct atomsnap_version) + sizeof(struct candle_row_page));

	version->object = row_page;
	version->free_context = mem_acc_arg;

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
	struct memory_accounting *mem_acc;

	if (version == NULL) {
		return;
	}

	mem_acc = (struct memory_accounting *)version->free_context;
	memstat_sub(&mem_acc->ms, MEMSTAT_CANDLE_CHUNK_LIST,
		sizeof(struct candle_row_page) + sizeof(struct atomsnap_version));

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
 * @param   mem_acc:            Memory accounting information.
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
struct candle_chunk *create_candle_chunk(trcache_candle_type candle_type,
	int symbol_id, int row_page_count, int batch_candle_count,
	struct memory_accounting *mem_acc)
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

	chunk->column_batch = trcache_batch_alloc_on_heap(batch_candle_count,
		TRCACHE_FIELD_MASK_ALL);
	if (chunk->column_batch == NULL) {
		errmsg(stderr, "Failure on trcache_batch_alloc_on_heap()\n");
		pthread_spin_destroy(&chunk->spinlock);
		atomsnap_destroy_gate(chunk->row_gate);
		free(chunk);
		return NULL;
	}

	memstat_add(&mem_acc->ms,
		MEMSTAT_CANDLE_CHUNK_LIST,
		sizeof(struct candle_chunk) +
			((sizeof(double) * TRCACHE_NUM_CANDLE_FIELD * batch_candle_count)));
	chunk->mem_acc = mem_acc;

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

	memstat_sub(&chunk->mem_acc->ms,
		MEMSTAT_CANDLE_CHUNK_LIST,
		sizeof(struct candle_chunk) +
			((sizeof(double) * TRCACHE_NUM_CANDLE_FIELD *
				chunk->column_batch->capacity)));

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
	const struct trcache_candle_update_ops *ops,
	struct trcache_trade_data *trade)
{
	struct candle_row_page *row_page = NULL;
	struct atomsnap_version *row_page_version
		= atomsnap_make_version(chunk->row_gate, (void *)chunk->mem_acc);

	if (row_page_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		return -1;
	}

	row_page = (struct candle_row_page *)row_page_version->object;

	/*
	 * Since the completion count has not been incremented yet, the candle
	 * initialization process is not visible to readers. Therefore, no locking
	 * is required.
	 */
	ops->init(&(row_page->rows[0]), trade);

	candle_chunk_write_start_timestamp(chunk, page_idx, 0,
		row_page->rows[0].start_timestamp);

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
	double *o_ptr = batch->open_array + start_idx;
	double *h_ptr = batch->high_array + start_idx;
	double *l_ptr = batch->low_array + start_idx;
	double *c_ptr = batch->close_array + start_idx;
	double *v_ptr = batch->volume_array + start_idx;
	double *tv_ptr = batch->trading_value_array + start_idx;
	uint32_t *tc_ptr = batch->trade_count_array + start_idx;
	bool *ic_ptr = batch->is_closed_array + start_idx;

	for (int idx = start_idx; idx <= end_idx; idx++) {
		next_page_idx = candle_chunk_calc_page_idx(idx);
		if (next_page_idx != cur_page_idx) {
			/*
			 * Page is fully converted. Trigger the grace period.
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
		*o_ptr++ = c->open;
		*h_ptr++ = c->high;
		*l_ptr++ = c->low;
		*c_ptr++ = c->close;
		*v_ptr++ = c->volume;
		*tv_ptr++ = c->trading_value;
		*tc_ptr++ = c->trade_count;
		*ic_ptr++ = c->is_closed;
	}
	atomsnap_release_version(page_version);

	/* This value is equal to chunk->num_completed */
	end_idx += 1;
	batch->num_candles = end_idx;

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
 * @param   flush_ops: User-defined batch flush operation callbacks.
 *
 * @return  1  flush completed synchronously  
 *          0  flush started asynchronously (still pending)  
 */
int candle_chunk_flush(struct trcache *trc, struct candle_chunk *chunk,
	const struct trcache_batch_flush_ops* flush_ops)
{
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
 * @param   trc:       Pointer to the parent trcache instance.
 * @param   chunk:     Pointer to the target candle_chunk.
 * @param   flush_ops: User-defined batch flush operation callbacks.
 *
 * @return  1  flush has completed *in this call*.
 *          0  flush has not completed *in this call*.
 */
int candle_chunk_flush_poll(struct trcache *trc, struct candle_chunk *chunk,
	const struct trcache_batch_flush_ops* flush_ops)
{
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

_Static_assert(TRCACHE_NUM_CANDLE_FIELD == 9, "Field count/dispatch mismatch");

/**
 * @brief   Dispatch-table based copier (GNU computed-goto).
 *
 * The function jumps directly to the code block that handles the first set-bit
 * in @mask, copies one field, clears that bit, and jumps again until all
 * requested fields are processed.
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
static inline void candle_copy_dispatch_tbl(
	struct trcache_candle * __restrict c,
	struct trcache_candle_batch * __restrict d,
	int i, trcache_candle_field_flags field_mask)
{
	static void *dispatch[TRCACHE_NUM_CANDLE_FIELD + 1] = {
		&&copy_start_ts,
		&&copy_open,
		&&copy_high,
		&&copy_low,
		&&copy_close,
		&&copy_volume,
		&&copy_trading_value,
		&&copy_trade_count,
		&&copy_is_closed,
		&&copy_done
	};
	uint32_t m;

	if (field_mask == 0) {
		return; /* nothing to copy */
	}

	/* Set the out bit */
	m = field_mask | (1 << TRCACHE_NUM_CANDLE_FIELD);
	goto *dispatch[__builtin_ctz(m)];

copy_start_ts:
	d->start_timestamp_array[i] = c->start_timestamp;
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
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_trading_value:
	d->trading_value_array[i] = c->trading_value;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_trade_count:
	d->trade_count_array[i] = c->trade_count;
	m &= m - 1;
	goto *dispatch[__builtin_ctz(m)];

copy_is_closed:
	d->is_closed_array[i] = c->is_closed;
	/* Fall through */

copy_done:
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
 * @param   idx:         Index of the candle to copy (0-based).
 * @param   dst_idx:     Index of the last element in @dst to fill.
 * @param   dst [out]:   Pre-allocated destination batch.
 * @param   field_mask:  Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_mutable_row(struct candle_chunk *chunk,
	int idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask)
{
	int page_idx = candle_chunk_calc_page_idx(idx);
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, page_idx);
	struct candle_row_page *row_page;
	struct trcache_candle *candle;
	int row_idx;

	if (page_version == NULL) {
		return 0;
	}

	row_page = (struct candle_row_page *)page_version->object;
	row_idx = candle_chunk_calc_row_idx(idx);
	candle = __builtin_assume_aligned(&row_page->rows[row_idx], 64);

	pthread_spin_lock(&chunk->spinlock);
	candle_copy_dispatch_tbl(candle, dst, dst_idx, field_mask);
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
 * @param   chunk:      Pointer to the candle_chunk.
 * @param   start_idx:  First candle index in the range (inclusive).
 * @param   end_idx:    Last candle index in the range (inclusive).
 * @param   dst_idx:    Index of the last element in @dst to fill.
 * @param   dst [out]:  Pre-allocated destination batch.
 * @param   field_mask: Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_rows_until_converted(struct candle_chunk *chunk,
	int start_idx, int end_idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask)
{
	int cur_page_idx = candle_chunk_calc_page_idx(end_idx);
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, cur_page_idx);
	struct candle_row_page *row_page;
	struct trcache_candle *candle;
	int next_page_idx, row_idx, num_copied = 0;

	if (page_version == NULL) {
		return 0;
	}

	row_page = (struct candle_row_page *)page_version->object;

	for (int idx = end_idx; idx >= start_idx; idx--) {
		next_page_idx = candle_chunk_calc_page_idx(idx);
		if (next_page_idx != cur_page_idx) {
			atomsnap_release_version(page_version);

			cur_page_idx = next_page_idx;
			page_version = atomsnap_acquire_version_slot(
				chunk->row_gate, cur_page_idx);

			if (page_version == NULL) {
				return num_copied;
			}

			row_page = (struct candle_row_page *)page_version->object;
		}

		row_idx = candle_chunk_calc_row_idx(idx);
		candle = __builtin_assume_aligned(&row_page->rows[row_idx], 64);
		candle_copy_dispatch_tbl(candle, dst, dst_idx, field_mask);

		num_copied += 1;
		dst_idx -= 1;
	}

	atomsnap_release_version(page_version);
	return num_copied;
}

/*
 * @brief   Copy a segment of memory containing 64‑bit values.
 *
 * This helper is tuned for copying arrays of 64‑bit types (e.g. uint64_t,
 * double). It requires that the byte count be a multiple of 8 and
 * performs a small unrolled loop when the segment is small. 
 * Larger segments fall back to memcpy().
 *
 * @param   src:   Source pointer.
 * @param   dst:   Destination pointer.
 * @param   bytes: Number of bytes to copy (must be a multiple of 8).
 */
static inline void copy_segment_u64(const void *__restrict src,
	void *__restrict dst, int bytes)
{
	assert((bytes & 7u) == 0);

	if (bytes <= 256) {
		const uint64_t *__restrict s = (const uint64_t *)src;
		uint64_t *__restrict d = (uint64_t *)dst;
		size_t n_qwords = (size_t)bytes >> 3;
		size_t i = 0;

		/* 64‑byte unroll */
		for (; i + 7 < n_qwords; i += 8) {
			d[i] = s[i];
			d[i + 1] = s[i + 1];
			d[i + 2] = s[i + 2];
			d[i + 3] = s[i + 3];
			d[i + 4] = s[i + 4];
			d[i + 5] = s[i + 5];
			d[i + 6] = s[i + 6];
			d[i + 7] = s[i + 7];
		}

		/* Remainder */
		for (; i < n_qwords; i++) {
			d[i] = s[i];
		}
	} else {
		memcpy(dst, src, (size_t)bytes);
	}
}

/*
 * @brief   Copy a segment of memory containing 32‑bit values.
 *
 * This helper mirrors copy_segment_u64() but operates on 4‑byte
 * elements such as uint32_t. The byte count must be a multiple of 4.
 *
 * @param   src:   Source pointer.
 * @param   dst:   Destination pointer.
 * @param   bytes: Number of bytes to copy (must be a multiple of 4).
 */
static inline void copy_segment_u32(const void *__restrict src,
	void *__restrict dst, int bytes)
{
	assert((bytes & 3u) == 0);

	if (bytes <= 256) {
		const uint32_t *__restrict s = (const uint32_t *)src;
		uint32_t *__restrict d = (uint32_t *)dst;
		size_t n_words = (size_t)bytes >> 2;
		size_t i = 0;

		/* 32‑byte unroll (8 words) */
		for (; i + 7 < n_words; i += 8) {
			d[i] = s[i];
			d[i + 1] = s[i + 1];
			d[i + 2] = s[i + 2];
			d[i + 3] = s[i + 3];
			d[i + 4] = s[i + 4];
			d[i + 5] = s[i + 5];
			d[i + 6] = s[i + 6];
			d[i + 7] = s[i + 7];
		}
		/* Remainder */
		for (; i < n_words; i++) {
			d[i] = s[i];
		}
	} else {
		memcpy(dst, src, (size_t)bytes);
	}
}

/*
 * @brief   Copy a segment of memory containing boolean values.
 *
 * This helper copies arrays of bool. Since bool values are 1 byte each,
 * there is no need for unrolling; memcpy() is sufficient and typically
 * optimized by the compiler. The byte count must be a multiple of 1.
 *
 * @param   src:   Source pointer.
 * @param   dst:   Destination pointer.
 * @param   bytes: Number of bytes to copy.
 */
static inline void copy_segment_bool(const void *__restrict src,
	void *__restrict dst, int bytes)
{
	/* For booleans there is no alignment requirement */
	memcpy(dst, src, (size_t)bytes);
}

/**
 * @brief   Copy candles that already reside in the column batch.
 *
 * Unlike the previous helpers, this routine bypasses row pages entirely and
 * pulls data from the chunk-local columnar storage area. It therefore
 * requires that the specified record range has *already* been converted.
 *
 * @param   chunk:       Pointer to the candle_chunk.
 * @param   start_idx:   First candle index in the range (inclusive).
 * @param   end_idx:     Last candle index in the range (inclusive).
 * @param   dst_idx:     Index of the last element in @dst to fill.
 * @param   dst [out]:   Pre-allocated destination batch.
 * @param   field_mask:  Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_from_column_batch(struct candle_chunk *chunk,
	int start_idx, int end_idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask)
{
	static void *const col_dispatch[TRCACHE_NUM_CANDLE_FIELD + 1] = {
		&&col_copy_start_ts,
		&&col_copy_open,
		&&col_copy_high,
		&&col_copy_low,
		&&col_copy_close,
		&&col_copy_volume,
		&&col_copy_trading_value,
		&&col_copy_trade_count,
		&&col_copy_is_closed,
		&&col_copy_done
	};
	int n = end_idx - start_idx + 1, dst_first = dst_idx + 1 - n;
	int bytes_db = n * sizeof(double), bytes_u64 = n * sizeof(uint64_t);
	int bytes_u32 = n * sizeof(uint32_t), bytes_bool = n * sizeof(bool);
	uint32_t m;

	if (n == 0 || field_mask == 0) {
		return 0;
	}

	/* Set the out bit */
	m = field_mask | (1 << TRCACHE_NUM_CANDLE_FIELD);
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_start_ts:
	copy_segment_u64(chunk->column_batch->start_timestamp_array + start_idx,
		dst->start_timestamp_array + dst_first, bytes_u64);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_open:
	copy_segment_u64(chunk->column_batch->open_array + start_idx,
		dst->open_array + dst_first, bytes_db);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_high:
	copy_segment_u64(chunk->column_batch->high_array + start_idx,
		dst->high_array + dst_first, bytes_db);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_low:
	copy_segment_u64(chunk->column_batch->low_array + start_idx,
		dst->low_array + dst_first, bytes_db);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_close:
	copy_segment_u64(chunk->column_batch->close_array + start_idx,
		dst->close_array + dst_first, bytes_db);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_volume:
	copy_segment_u64(chunk->column_batch->volume_array + start_idx,
		dst->volume_array + dst_first, bytes_db);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_trading_value:
	copy_segment_u64(chunk->column_batch->trading_value_array + start_idx,
		dst->trading_value_array + dst_first, bytes_db);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_trade_count:
	copy_segment_u32(chunk->column_batch->trade_count_array + start_idx,
		dst->trade_count_array + dst_first, bytes_u32);
	m &= m - 1;
	goto *col_dispatch[__builtin_ctz(m)];

col_copy_is_closed:
	copy_segment_bool(chunk->column_batch->is_closed_array + start_idx,
		dst->is_closed_array + dst_first, bytes_bool);
	/* Fall through */

col_copy_done:
	return n;
}
