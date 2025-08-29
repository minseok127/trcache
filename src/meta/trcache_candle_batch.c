/**
 * @file   trcache_candle_batch.c
 * @brief  Heap-side implementation of contiguous, SIMD-aligned batches.
 */

#include <stdio.h>
#include <string.h>

#include "utils/log.h"

#include "trcache.h"

/**
 * @brief   Allocate an @p align-byte-aligned memory block.
 *
 * Wraps 'aligned_alloc', 'posix_memalign', or '_aligned_malloc'
 * depending on platform capabilities.
 *
 * @param   align: Alignment in bytes (must be a power-of-two).
 * @param   bytes: Requested size in bytes.
 *
 * @return  Pointer on success, 'NULL' on failure.
 */
static void *simd_aligned_alloc(size_t align, size_t bytes)
{
#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	size_t sz = (bytes + align - 1) & ~(align - 1);
	return aligned_alloc(align, sz);
#else
	void *p = NULL;
	return (posix_memalign(&p, align, bytes) == 0) ? p : NULL;
#endif
}

static void simd_aligned_free(void *p)
{
	free(p);
}

static size_t align_up(size_t x, size_t a)
{
	return (x + a - 1) & ~(a - 1);
}

/**
 * @brief   Allocate a contiguous, SIMD-aligned candle batch on the heap.
 *
 * @param   capacity:   Number of OHLCV rows to allocate (must be > 0).
 * @param   field_mask: OR-ed set of required fields.
 *
 * @return  Pointer to a fully-initialised #trcache_candle_batch on success,
 *          'NULL' on allocation failure or invalid *capacity*.
 *
 * @note    The returned pointer must be released via trcache_batch_free().
 */
struct trcache_candle_batch *trcache_batch_alloc_on_heap(int capacity,
	trcache_candle_field_flags field_mask)
{
	const size_t a = TRCACHE_SIMD_ALIGN;
	size_t off_start_ts = 0;
	size_t off_open = 0, off_high = 0, off_low = 0, off_close = 0, off_vol = 0;
	size_t off_tv = 0, off_tc = 0, off_ic = 0;
	size_t off_struct, total_sz, u64b, dblb, u32b, boolb;
	struct trcache_candle_batch *b;
	void *base;

	if (capacity <= 0) {
		errmsg(stderr, "Invalid argument (capacity <= 0)\n");
		return NULL;
	}

	off_struct = align_up(sizeof(struct trcache_candle_batch), a);

	u64b = (size_t)capacity * sizeof(uint64_t);
	dblb = (size_t)capacity * sizeof(double);
	u32b = (size_t)capacity * sizeof(uint32_t);
	boolb = (size_t)capacity * sizeof(bool);

	/* Compute offsets for each array, respecting alignment padding. */
	size_t cur = off_struct;

	if (field_mask & TRCACHE_START_TIMESTAMP) {
		off_start_ts = cur;
		cur = align_up(cur + u64b, a);
	}

	if (field_mask & TRCACHE_OPEN) {
		off_open = cur;
		cur = align_up(cur + dblb, a);
	}
	
	if (field_mask & TRCACHE_HIGH) {
		off_high = cur;
		cur = align_up(cur + dblb, a);
	}
	
	if (field_mask & TRCACHE_LOW) {
		off_low = cur;
		cur = align_up(cur + dblb, a);
	}
	
	if (field_mask & TRCACHE_CLOSE) {
		off_close = cur;
		cur = align_up(cur + dblb, a);
	}
	
	if (field_mask & TRCACHE_VOLUME) {
		off_vol = cur;
		cur = align_up(cur + dblb, a);
	}

	if (field_mask & TRCACHE_TRADING_VALUE) {
		off_tv = cur;
		cur = align_up(cur + dblb, a);
	}

	if (field_mask & TRCACHE_TRADE_COUNT) {
		off_tc = cur;
		cur = align_up(cur + u32b, a);
	}

	if (field_mask & TRCACHE_IS_CLOSED) {
		off_ic = cur;
		cur = align_up(cur + boolb, a);
	}

	total_sz = align_up(cur, a);

	/* Single aligned block for struct + all arrays. */
	base = simd_aligned_alloc(a, total_sz);

	if (base == NULL) {
		errmsg(stderr, "Failure on simd_aligned_alloc()\n");
		return NULL;
	}

	/* Wire up internal pointers. */
	b = (struct trcache_candle_batch *)base;

	b->num_candles = 0;
	b->capacity = capacity;
	b->symbol_id = -1;

	b->start_timestamp_array = (field_mask & TRCACHE_START_TIMESTAMP) ?
		(uint64_t *)((uint8_t *)base + off_start_ts) : NULL;
	b->open_array = (field_mask & TRCACHE_OPEN) ?
		(double *)((uint8_t *)base + off_open) : NULL;
	b->high_array = (field_mask & TRCACHE_HIGH) ?
		(double *)((uint8_t *)base + off_high) : NULL;
	b->low_array = (field_mask & TRCACHE_LOW) ?
		(double *)((uint8_t *)base + off_low) : NULL;
	b->close_array = (field_mask & TRCACHE_CLOSE) ?
		(double *)((uint8_t *)base + off_close) : NULL;
	b->volume_array = (field_mask & TRCACHE_VOLUME) ?
		(double *)((uint8_t *)base + off_vol) : NULL;
	b->trading_value_array = (field_mask & TRCACHE_TRADING_VALUE) ?
		(double *)((uint8_t *)base + off_tv) : NULL;
	b->trade_count_array = (field_mask & TRCACHE_TRADE_COUNT) ?
		(uint32_t *)((uint8_t *)base + off_tc) : NULL;
	b->is_closed_array = (field_mask & TRCACHE_IS_CLOSED) ?
		(bool *)((uint8_t *)base + off_ic) : NULL;

	return b;
}

/**
 * @brief   Release a heap-allocated candle batch.
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 *
 * @param   batch: Pointer obtained from trcache_batch_alloc_on_heap().
 */
void trcache_batch_free(struct trcache_candle_batch *b)
{
	if (b != NULL) {
		simd_aligned_free((void *)b); /* Struct address == block base */
	}
}

