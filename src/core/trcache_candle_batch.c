/**
 * @file   trcache_candle_batch.c
 * @brief  Heap-side implementation of contiguous, SIMD-aligned batches.
 */

#include <string.h>

#include "trcache.h"

/**
 * @brief  Allocate an @p align-byte-aligned memory block.
 *
 * Wraps 'aligned_alloc', 'posix_memalign', or '_aligned_malloc'
 * depending on platform capabilities.
 *
 * @param align: Alignment in bytes (must be a power-of-two).
 * @param bytes: Requested size in bytes.
 *
 * @return Pointer on success, 'NULL' on failure.
 */
static void *simd_aligned_alloc(size_t align, size_t bytes)
{
#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	size_t sz = (bytes + align - 1) & ~(align - 1);
	return aligned_alloc(align, sz);
#elif defined(_POSIX_VERSION)
	void *p = NULL;
	return (posix_memalign(&p, align, bytes) == 0) ? p : NULL;
#elif defined(_WIN32)
	return _aligned_malloc(bytes, align);
#else
#error "No aligned allocation routine available for this platform."
#endif
}

static void simd_aligned_free(void *p)
{
#if defined(_WIN32)
	_aligned_free(p);
#else
	free(p);
#endif
}

static size_t align_up(size_t x, size_t a)
{
	return (x + a - 1) & ~(a - 1);
}

/**
 * @brief  Allocate a contiguous, SIMD-aligned candle batch on the heap.
 *
 * @param n: Number of OHLCV rows to allocate (must be > 0).
 *
 * @return Pointer to a fully-initialised #trcache_candle_batch on success,  
 *         'NULL' on allocation failure or invalid *num_candles*.
 *
 * @note The returned pointer must be released via trcache_batch_free().
 */
struct trcache_candle_batch *trcache_batch_alloc_on_heap(int n)
{
	const size_t a = TRCACHE_SIMD_ALIGN;
	size_t off_first, off_last, off_open, off_high, off_low, off_close, off_vol;
	size_t off_struct, total_sz, u64b, dblb;
	struct trcache_candle_batch *b;
	void *base;

	if (n <= 0) {
		fprintf(stderr, "trcache_batch_alloc_on_heap: invalid n\n");
		return NULL;
	}

	off_struct = align_up(sizeof(struct trcache_candle_batch), a);

	u64b = (size_t)n * sizeof(uint64_t);
	dblb = (size_t)n * sizeof(double);

	/* Compute offsets for each array, respecting alignment padding. */
	off_first = off_struct;
	off_last = align_up(off_first + u64b, a);
	off_open = align_up(off_last + u64b, a);
	off_high = align_up(off_open + dblb, a);
	off_low = align_up(off_high + dblb, a);
	off_close = align_up(off_low + dblb, a);
	off_vol = align_up(off_close + dblb, a);
	total_sz = align_up(off_vol + dblb, a);

	/* Single aligned block for struct + all arrays. */
	base = simd_aligned_alloc(a, total_sz);

	if (base == NULL) {
		fprintf(stderr, "trcache_batch_alloc_on_heap: alloc failed\n");
		return NULL;
	}

	/* Wire up internal pointers. */
	b = (struct trcache_candle_batch *)base;
	memset(b, 0, sizeof *b);
	b->num_candles = n;
	b->first_timestamp_array = (uint64_t *)((uint8_t *)base + off_first);
	b->last_timestamp_array = (uint64_t *)((uint8_t *)base + off_last);
	b->open_array = (double *)((uint8_t *)base + off_open);
	b->high_array = (double *)((uint8_t *)base + off_high);
	b->low_array = (double *)((uint8_t *)base + off_low);
	b->close_array = (double *)((uint8_t *)base + off_close);
	b->volume_array = (double *)((uint8_t *)base + off_vol);

	return b; /* Arrays are uninitialised; memset if zeroing is needed. */
}

/**
 * @brief  Release a heap-allocated candle batch.
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 *
 * @param batch: Pointer obtained from trcache_batch_alloc_on_heap().
 */
void trcache_batch_free(struct trcache_candle_batch *b)
{
	if (b != NULL) {
		simd_aligned_free((void *)b); /* Struct address == block base */
	}
}

