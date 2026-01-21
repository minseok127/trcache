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

static size_t align_up(size_t x, size_t a)
{
	return (x + a - 1) & ~(a - 1);
}

/*
 * Helper function for memory accounting.
 */
static size_t calculate_batch_total_size(struct trcache *trc,
	int candle_idx, const struct trcache_candle_batch *batch)
{
	size_t total_size = 0;
	const struct trcache_candle_config *config
		= &trc->candle_configs[candle_idx];

	if (batch == NULL) {
		return 0;
	}

	total_size = align_up(sizeof(struct trcache_candle_batch),
		TRCACHE_SIMD_ALIGN);
	total_size = align_up(
		total_size + sizeof(void *) * config->num_fields,
		TRCACHE_SIMD_ALIGN);
	total_size = align_up(
		total_size + (size_t)batch->capacity * sizeof(uint64_t),
		TRCACHE_SIMD_ALIGN); // key_array
	total_size = align_up(
		total_size + (size_t)batch->capacity * sizeof(bool),
		TRCACHE_SIMD_ALIGN); // is_closed_array

	for (int i = 0; i < config->num_fields; i++) {
		if (batch->column_arrays[i]) {
			total_size = align_up(total_size + 
				(size_t)batch->capacity * config->field_definitions[i].size,
				TRCACHE_SIMD_ALIGN);
		}
	}

	return total_size;
}

/**
 * @brief   Allocate a candle row page object.
 *
 * @param   chunk: Pointer to the parent #candle_chunk struct.
 *
 * The returned pointer is a zero‑initialized, 64‑byte‑aligned row page.
 *
 * @return  Pointer to candle_row_page, or NULL on failure.
 */
static struct candle_row_page *alloc_row_page_object(struct candle_chunk *chunk)
{
	struct scalable_queue *row_page_pool = chunk->row_page_pool;
	struct candle_row_page *row_page = NULL;

	/* 1. Try to get from pool */
	scq_dequeue(row_page_pool, (void **)&row_page);

	/* 2. Pool empty or memory pressure, allocate new one */
	if (row_page == NULL) {
		row_page = aligned_alloc(TRCACHE_SIMD_ALIGN, chunk->row_page_mem_size);
		if (row_page == NULL) {
			errmsg(stderr, "#candle_row_page allocation failed\n");
			return NULL;
		}

		/*
		 * Track memory for the new page.
		 */
		mem_add_atomic(&row_page_pool->object_memory_usage.value,
			chunk->row_page_mem_size);
	}

	memset(row_page, 0, sizeof(struct candle_row_page));
	row_page->pool = row_page_pool;

	return row_page;
}

/**
 * @brief   Final cleanup for a row page object.
 *
 * @param   object:       Pointer to the #candle_row_page.
 * @param   free_context: Pointer to the #candle_chunk.
 *
 * Called by atomsnap when the reference count drops to zero.
 */
static void free_row_page_object(void *object, void *free_context)
{
	struct candle_chunk *chunk = (struct candle_chunk *)free_context;
	struct trcache *trc = chunk->trc;
	struct candle_row_page *row_page = (struct candle_row_page *)object;
	struct scalable_queue *row_page_pool = row_page->pool;
	bool memory_pressure;

	if (row_page == NULL) {
		return;
	}

	memory_pressure = atomic_load_explicit(
		&trc->mem_acc.memory_pressure, memory_order_acquire);

	if (memory_pressure) {
		/* Free the page and update object memory tracking */
		mem_sub_atomic(&row_page_pool->object_memory_usage.value,
			chunk->row_page_mem_size);
		free(row_page);
	} else {
		/* Return the page to the pool */
		scq_enqueue(row_page_pool, row_page);
	}
}

/**
 * @brief   Allocate and initialize #candle_chunk.
 *
 * @param   trc:                Pointer to the main trcache instance.
 * @param   candle_idx:         Candle type index of the column-batch.
 * @param   symbol_id:          Symbol ID of the column-batch.
 * @param   row_page_count:     Number of row pages per chunk.
 * @param   batch_candle_count: Number of candles per chunk.
 * @param   chunk_pool:         SCQ pool for recycling chunks.
 * @param   row_page_pool:      SCQ pool for recycling row pages.
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
struct candle_chunk *create_candle_chunk(struct trcache *trc,
	int candle_idx, int symbol_id, int row_page_count, int batch_candle_count,
	struct scalable_queue *chunk_pool, struct scalable_queue *row_page_pool)
{
	struct candle_chunk *chunk = NULL;
	struct atomsnap_init_context ctx = {
		.free_impl = free_row_page_object,
		.num_extra_control_blocks = row_page_count - 1
	};
	size_t batch_total_size = 0;
	size_t chunk_total_size = 0;
	bool allocated_new = false;

	/* 1. Try to get from pool */
	scq_dequeue(chunk_pool, (void **)&chunk);

	/* 2. Pool empty or memory pressure, allocate new one */
	if (chunk == NULL) {
		allocated_new = true;
		chunk = aligned_alloc(CACHE_LINE_SIZE, 
			align_up(sizeof(struct candle_chunk), CACHE_LINE_SIZE));
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

		chunk->column_batch = trcache_batch_alloc_on_heap(trc, candle_idx,
			batch_candle_count, NULL);
		if (chunk->column_batch == NULL) {
			errmsg(stderr, "Failure on trcache_batch_alloc_on_heap()\n");
			pthread_spin_destroy(&chunk->spinlock);
			atomsnap_destroy_gate(chunk->row_gate);
			free(chunk);
			return NULL;
		}
	} else {
		/*
		 * RECYCLED CHUNK: Reset only what's necessary.
		 * - spinlock is already initialized.
		 * - column_batch is already allocated.
		 * - row_gate is already initialized and is clean.
		 *
		 * Just reset the batch counter.
		 */
		chunk->column_batch->num_candles = 0;
	}

	/* 3. Common initialization (for both new and recycled) */
	chunk->trc = trc;
	chunk->chunk_pool = chunk_pool;
	chunk->row_page_pool = row_page_pool;
	chunk->row_page_count = row_page_count;
	chunk->column_batch->symbol_id = symbol_id;
	chunk->column_batch->candle_idx = candle_idx;

	/* Calculate and store memory sizes */
	batch_total_size = calculate_batch_total_size(trc,
		candle_idx, chunk->column_batch);
	chunk_total_size = sizeof(struct candle_chunk) + batch_total_size;
	
	chunk->chunk_mem_size = chunk_total_size;
	chunk->row_page_mem_size = align_up(
		sizeof(struct candle_row_page) + 
			(TRCACHE_ROWS_PER_PAGE * 
				trc->candle_configs[candle_idx].user_candle_size),
		TRCACHE_SIMD_ALIGN);

	if (allocated_new) {
		mem_add_atomic(&chunk_pool->object_memory_usage.value,
			chunk_total_size);
	}

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
	struct trcache *trc;
	bool memory_pressure;

	if (chunk == NULL) {
		return;
	}

	/*
	 * Explicitly release all row page versions to trigger their cleanup.
	 */
	for (int i = 0; i < chunk->row_page_count; i++) {
		atomsnap_exchange_version_slot(chunk->row_gate, i, NULL);
	}

	trc = chunk->trc;
	memory_pressure = atomic_load_explicit(
		&trc->mem_acc.memory_pressure, memory_order_acquire);

	if (memory_pressure) {
		mem_sub_atomic(&chunk->chunk_pool->object_memory_usage.value,
			chunk->chunk_mem_size);
		pthread_spin_destroy(&chunk->spinlock);
		atomsnap_destroy_gate(chunk->row_gate);
		trcache_batch_free(chunk->column_batch);
		free(chunk);
	} else {
		scq_enqueue(chunk->chunk_pool, chunk);
	}
}

/**
 * @brief   Initialize a row page within a candle chunk.
 *
 * @param   chunk:           Pointer to the candle chunk.
 * @param   page_idx:        Index of the page to initialize.
 * @param   ops:             Callback operations for candle initialization.
 * @param   trade:           First trade data used to initialize the candle.
 * @param   first_key (out): Pointer to store the key of the first candle.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_page_init(struct candle_chunk *chunk, int page_idx,
	const struct trcache_candle_update_ops *ops,
	void *trade, uint64_t *first_key)
{
	struct candle_row_page *row_page = NULL;
	struct atomsnap_version *row_page_version = NULL;
	struct trcache_candle_base *first_candle;

	row_page_version = atomsnap_make_version(chunk->row_gate);
	if (row_page_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		return -1;
	}

	row_page = alloc_row_page_object(chunk);
	if (row_page == NULL) {
		errmsg(stderr, "Failure on alloc_row_page_object()\n");
		atomsnap_free_version(row_page_version);
		return -1;
	}

	atomsnap_set_object(row_page_version, row_page, chunk);

	first_candle = (struct trcache_candle_base *)row_page->data;

	/*
	 * Since the completion count has not been incremented yet, the candle
	 * initialization process is not visible to readers. Therefore, no locking
	 * is required.
	 */
	ops->init(first_candle, trade);

	*first_key = first_candle->key.value;

	candle_chunk_write_key(chunk, page_idx, 0, *first_key);

	atomsnap_exchange_version_slot(chunk->row_gate, page_idx, row_page_version);

	return 0;
}

/**
 * @brief   Helper to copy a single row-oriented candle to a columnar batch,
 *          copying all user-defined fields.
 */
static inline void copy_row_to_batch_all(const trcache_candle_base *candle,
	struct trcache_candle_batch *dst, int dst_idx, struct trcache *trc,
	int candle_type_idx)
{
	const struct trcache_candle_config *config
		= &trc->candle_configs[candle_type_idx];
	void *dst_col, *src_field, *dst_field;
	const struct trcache_field_def *field;

	/* Always copy base fields */
	dst->key_array[dst_idx] = candle->key.value;
	dst->is_closed_array[dst_idx] = candle->is_closed;

	/* Copy all user fields */
	for (int i = 0; i < config->num_fields; i++) {
		dst_col = dst->column_arrays[i];
		if (dst_col != NULL) {
			field = &config->field_definitions[i];
			src_field = (char *)candle + field->offset;
			dst_field = (char *)dst_col + (dst_idx * field->size);
			memcpy(dst_field, src_field, field->size);
		}
	}
}

/**
 * @brief   Helper to copy a single row-oriented candle to a columnar batch,
 *          copying only the requested user-defined fields.
 */
static inline void copy_row_to_batch_selective(
	const trcache_candle_base *candle, struct trcache_candle_batch *dst,
	int dst_idx, const trcache_field_request *request, struct trcache *trc,
	int candle_type_idx)
{
	const struct trcache_candle_config *config
		= &trc->candle_configs[candle_type_idx];
	const struct trcache_field_def *field;
	void *src_field, *dst_field;
	int field_idx;

	/* Always copy base fields */
	dst->key_array[dst_idx] = candle->key.value;
	dst->is_closed_array[dst_idx] = candle->is_closed;

	for (int i = 0; i < request->num_fields; i++) {
		field_idx = request->field_indices[i];
		field = &config->field_definitions[field_idx];
		
		src_field
			= (char *)candle + field->offset;
		dst_field
			= (char *)dst->column_arrays[field_idx] + (dst_idx * field->size);
		
		memcpy(dst_field, src_field, field->size);
	}
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
	struct trcache *trc = chunk->trc;
	int candle_type_idx = batch->candle_idx;
	const struct trcache_candle_config *config
		= &trc->candle_configs[candle_type_idx];
	int cur_page_idx = chunk->converting_page_idx;
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, cur_page_idx);
	struct candle_row_page *page
		= (struct candle_row_page *)atomsnap_get_object(page_version);

	for (int idx = start_idx; idx <= end_idx; idx++) {
		int next_page_idx = candle_chunk_calc_page_idx(idx);

		if (next_page_idx != cur_page_idx) {
			/*
			 * If we should move to the next page, reclaim the previous page.
			 */
			atomsnap_exchange_version_slot(
				chunk->row_gate, cur_page_idx, NULL);
			atomsnap_release_version(page_version);

			/* Move to the next page */
			cur_page_idx = next_page_idx;

			page_version = atomsnap_acquire_version_slot(
				chunk->row_gate, cur_page_idx);
			page = (struct candle_row_page *)atomsnap_get_object(page_version);
		}

		char *src_candle_base = page->data +
			(candle_chunk_calc_row_idx(idx) * config->user_candle_size);
		struct trcache_candle_base *c
			= (struct trcache_candle_base *)src_candle_base;

		copy_row_to_batch_all(c, batch, idx, trc, candle_type_idx);
	}

	/*
	 * Page is fully converted. Reclaim it.
	 */
	if ((cur_page_idx != candle_chunk_calc_page_idx(end_idx + 1)
			|| ((end_idx + 1) == batch->capacity))) {
		atomsnap_exchange_version_slot(chunk->row_gate, cur_page_idx, NULL);
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
 * @param   chunk:     Pointer to the target candle_chunk.
 * @param   flush_ops: User-defined batch flush operation callbacks.
 *
 * @return  1  flush completed synchronously  
 *          0  flush started asynchronously (still pending)  
 */
int candle_chunk_flush(struct candle_chunk *chunk,
	const struct trcache_batch_flush_ops* flush_ops)
{
	struct trcache *trc = chunk->trc;

	/* Flush operation is not defined, do nothing */
	if (flush_ops->flush == NULL) {
		chunk->is_flushed = 1;
		return 1;
	}
	
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
 * @param   chunk:     Pointer to the target candle_chunk.
 * @param   flush_ops: User-defined batch flush operation callbacks.
 *
 * @return  1  flush has completed *in this call*.
 *          0  flush has not completed *in this call*.
 */
int candle_chunk_flush_poll(struct candle_chunk *chunk,
	const struct trcache_batch_flush_ops* flush_ops)
{
	struct trcache *trc = chunk->trc;

	/* Synchronous flush already finished earlier. */
	if (chunk->is_flushed) {
		return 0;
	}

	/* Ask the backend whether the I/O has finished. */
	if (flush_ops->is_done(trc, chunk->column_batch, chunk->flush_handle)) {
		/* Tear down backend resources and mark completion. */
		flush_ops->destroy_async_handle(chunk->flush_handle,
			flush_ops->destroy_async_handle_ctx);
		chunk->flush_handle = NULL;
		chunk->is_flushed = 1;
		return 1;
	}

	/* Still pending. */
	return 0;
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
 * @param   request:     Specifies which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_mutable_row(struct candle_chunk *chunk,
	int idx, int dst_idx, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	struct trcache *trc = chunk->trc;
	int candle_type_idx = chunk->column_batch->candle_idx;
	const struct trcache_candle_config *config
		= &trc->candle_configs[candle_type_idx];	
	int page_idx = candle_chunk_calc_page_idx(idx);
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, page_idx);
	struct candle_row_page *row_page;
	char *src_candle_base;
	struct trcache_candle_base *candle;
	int row_idx;

	if (page_version == NULL) {
		return 0;
	}

	row_page = (struct candle_row_page *)atomsnap_get_object(page_version);
	row_idx = candle_chunk_calc_row_idx(idx);

	src_candle_base = row_page->data + (row_idx * config->user_candle_size);
	candle = (struct trcache_candle_base *)src_candle_base;

	pthread_spin_lock(&chunk->spinlock);
	copy_row_to_batch_selective(candle, dst, dst_idx, request,
		chunk->trc, candle_type_idx);
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
 * @param   request:     Specifies which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_rows_until_converted(struct candle_chunk *chunk,
	int start_idx, int end_idx, int dst_idx, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	struct trcache *trc = chunk->trc;
	int candle_type_idx = chunk->column_batch->candle_idx;
	const struct trcache_candle_config *config
		= &trc->candle_configs[candle_type_idx];
	int cur_page_idx = candle_chunk_calc_page_idx(end_idx);
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, cur_page_idx);
	struct candle_row_page *row_page;
	char *src_candle_base;
	struct trcache_candle_base *candle;
	int next_page_idx, row_idx, num_copied = 0;

	if (page_version == NULL) {
		return 0;
	}

	row_page = (struct candle_row_page *)atomsnap_get_object(page_version);

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

			row_page = (struct candle_row_page *)
				atomsnap_get_object(page_version);
		}

		row_idx = candle_chunk_calc_row_idx(idx);

		src_candle_base = row_page->data + (row_idx * config->user_candle_size);
		candle = (struct trcache_candle_base *)src_candle_base;

		copy_row_to_batch_selective(candle, dst, dst_idx, request,
			chunk->trc, candle_type_idx);

		num_copied += 1;
		dst_idx -= 1;
	}

	atomsnap_release_version(page_version);
	return num_copied;
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
 * @param   request:     Specifies which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_from_column_batch(struct candle_chunk *chunk,
	int start_idx, int end_idx, int dst_idx, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	int n = end_idx - start_idx + 1;
	int dst_first = dst_idx + 1 - n;
	int candle_type_idx = chunk->column_batch->candle_idx;
	const struct trcache_candle_config *config
		= &chunk->trc->candle_configs[candle_type_idx];
	const struct trcache_field_def *field;
	void *dst_col, *src_col;
	int field_idx;

	if (n <= 0) {
		return 0;
	}

	/* Always copy base fields */
	memcpy(dst->key_array + dst_first,
	       chunk->column_batch->key_array + start_idx,
	       n * sizeof(uint64_t));
	memcpy(dst->is_closed_array + dst_first,
	       chunk->column_batch->is_closed_array + start_idx,
	       n * sizeof(bool));

	for (int i = 0; i < request->num_fields; i++) {
		field_idx = request->field_indices[i];
		field = &config->field_definitions[field_idx];
		
		dst_col = dst->column_arrays[field_idx];
		src_col = chunk->column_batch->column_arrays[field_idx];
		
		memcpy((char *)dst_col + (dst_first * field->size),
		       (char *)src_col + (start_idx * field->size),
		       n * field->size);
	}

	return n;
}
