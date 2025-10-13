/**
 * @file   trcache_candle_batch.c
 * @brief  Heap-side implementation of contiguous, SIMD-aligned batches.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>

#include "meta/trcache_internal.h"
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
 * @param   tc:         Pointer to the trcache instance.
 * @param   candle_idx: Index of the candle configuration to use for layout.
 * @param   capacity:   Number of candle rows to allocate (must be > 0).
 * @param   request:    Pointer to a struct specifying which fields to allocate.
 *                      If NULL, all fields defined at init are allocated.
 *                      Base fields are always included.
 *
 * @return  Pointer to a fully-initialised #trcache_candle_batch on success,
 *          'NULL' on allocation failure or invalid *capacity*.
 *
 * @note    The returned pointer must be released via trcache_batch_free().
 */
struct trcache_candle_batch *trcache_batch_alloc_on_heap(struct trcache *tc,
	int candle_idx, int capacity, const struct trcache_field_request *request)
{
	const struct trcache_candle_config *config
		= &tc->candle_configs[candle_idx];
	const size_t a = TRCACHE_SIMD_ALIGN;
	struct trcache_candle_batch *b;
	size_t total_sz, col_array_ptr_sz;
	bool *requested_fields;
	uint8_t *p;
	void *base;
	int field_idx;

	if (capacity <= 0) {
		errmsg(stderr, "Invalid argument (capacity <= 0)\n");
		return NULL;
	}

	/* Determine which fields to allocate */
	requested_fields = alloca(sizeof(bool) * config->num_fields);

	if (request == NULL) {
		for (int i = 0; i < config->num_fields; i++) {
			requested_fields[i] = true;
		}
	} else {
		for (int i = 0; i < config->num_fields; i++) {
			requested_fields[i] = false;
		}

		for (int i = 0; i < request->num_fields; i++) {
			field_idx = request->field_indices[i];
		
			if (field_idx >= 0 && field_idx < config->num_fields) {
				requested_fields[field_idx] = true;
			}
		}
	}

	/* Calculate total size for the single memory block */
	total_sz = align_up(sizeof(struct trcache_candle_batch), a);
	col_array_ptr_sz = sizeof(void *) * config->num_fields;
	total_sz = align_up(total_sz + col_array_ptr_sz, a);

	/* Base fields are always allocated */
	total_sz = align_up(total_sz + (size_t)capacity * sizeof(uint64_t), a);
	total_sz = align_up(total_sz + (size_t)capacity * sizeof(bool), a);

	/* User-defined fields */
	for (int i = 0; i < config->num_fields; i++) {
		if (requested_fields[i]) {
			total_sz = align_up(total_sz +
				(size_t)capacity * config->field_definitions[i].size, a);
		}
	}

	/* Single aligned block for struct + all arrays. */
	base = simd_aligned_alloc(a, total_sz);
	if (base == NULL) {
		errmsg(stderr, "Failure on simd_aligned_alloc()\n");
		return NULL;
	}
	p = (uint8_t *)base;

	/* Wire up internal pointers. */
	b = (struct trcache_candle_batch *)p;
	p += align_up(sizeof(struct trcache_candle_batch), a);

	b->column_arrays = (void **)p;
	p += align_up(col_array_ptr_sz, a);

	b->key_array = (uint64_t *)p;
	p += align_up((size_t)capacity * sizeof(uint64_t), a);

	b->is_closed_array = (bool *)p;
	p += align_up((size_t)capacity * sizeof(bool), a);

	for (int i = 0; i < config->num_fields; i++) {
		if (requested_fields[i]) {
			b->column_arrays[i] = (void *)p;
			p += align_up((size_t)capacity *
					config->field_definitions[i].size, a);
		} else {
			b->column_arrays[i] = NULL;
		}
	}

	b->num_candles = 0;
	b->capacity = capacity;
	b->symbol_id = -1;
	b->candle_idx = candle_idx;

	return b;
}

/**
 * @brief   Release a heap-allocated candle batch.
 *
 * This function is intended for batches allocated by the user via
 * trcache_batch_alloc_on_heap(). It only frees the memory block.
 * It does *not* invoke the on_batch_destroy() callback, which is reserved
 * for the internal pipeline. The user is responsible for cleaning up any
 * custom resources within the batch before calling this function.
 *
 * @param   batch: Pointer to the batch to be freed.
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 */
void trcache_batch_free(struct trcache_candle_batch *b)
{
	if (b != NULL) {
		simd_aligned_free((void *)b); /* Struct address == block base */
	}
}

