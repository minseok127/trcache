/* 
 * @file   candle_chunk_list.c
 * @brief  Implementation of candle_chunk_list
 *
 * This module manages multiple candle chunks using a linked list. All reading,
 * updating, and flushing of candle_chunk by worker threads go through this
 * module.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>
#include <inttypes.h>

#include "meta/trcache_internal.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/log.h"

#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))

/**
 * @brief   Allocate an candle chunk list's head version object.
 *
 * @param   chunk_list: #candle_chunk_list pointer.
 *
 * @return  Pointer to #candle_chunk_list_head_version, or NULL on failure.
 */
static struct candle_chunk_list_head_version *alloc_head_version_object(
	struct candle_chunk_list *list)
{
	struct trcache *trc = list->trc;
	struct candle_chunk_list_head_version *head_ver = NULL;
	size_t size = sizeof(struct candle_chunk_list_head_version);

	/* 1. Try to get from pool */
	scq_dequeue(trc->head_version_pool, (void **)&head_ver);

	/* 2. Pool empty or memory pressure, allocate new one */
	if (head_ver == NULL) {
		head_ver = malloc(size);
		if (head_ver == NULL) {
			errmsg(stderr, "#candle_chunk_list_head_version alloc failed\n");
			return NULL;
		}

		/* Track memory for the new object */
		mem_add_atomic(&trc->head_version_pool->object_memory_usage.value,
			size);
	}

	/* 3. Initialize and return */
	memset(head_ver, 0, size);
	
	return head_ver;
}

#define HEAD_VERSION_RELEASE_MASK (0x8000000000000000ULL)

/**
 * @brief   Frees the chunks covered by the given head version.
 *
 * @param   object:       The #candle_chunk_list_head_version object.
 * @param   free_context: The #candle_chunk_list pointer.
 *
 * This function implements a "chain-freeing" mechanism. When a head version
 * is freed, it checks if it's the current end of the retired list (i.e., its
 * prev pointer is NULL). If so, it frees its associated chunks.
 *
 * After freeing its own chunks, it attempts to free the *next* head version
 * in the chain. This is necessary because the thread that retired the next
 * version might have returned early, delegating the cleanup responsibility.
 *
 * The race between multiple threads trying to free adjacent versions
 * is handled by atomically marking the `head_version_prev` pointer using
 * `atomic_fetch_or` and then using `atomic_compare_exchange_weak`.
 * This ensures that only one thread can successfully claim the responsibility
 * for freeing a subsequent version in the chain, preventing double-frees.
 */
static void candle_chunk_list_head_free(void *object, void *free_context)
{
	struct candle_chunk_list *chunk_list
		= (struct candle_chunk_list *)free_context;
	struct trcache *trc = chunk_list->trc;
	struct candle_chunk_index *chunk_index = chunk_list->chunk_index;
	struct candle_chunk_list_head_version *head_version
		= (struct candle_chunk_list_head_version *)object;
	struct candle_chunk_list_head_version *next_head_version = NULL;
	struct candle_chunk_list_head_version *prev_head_version = NULL;
	struct candle_chunk *prev_chunk = NULL, *chunk = NULL;
	bool memory_pressure;
#ifdef TRCACHE_DEBUG
	struct candle_chunk *pop_head = NULL; /* debug purpose */
#endif /* TRCACHE_DEBUG */

	prev_head_version
		= (struct candle_chunk_list_head_version *) atomic_fetch_or(
			&head_version->head_version_prev, HEAD_VERSION_RELEASE_MASK);

	/*
	 * This is not the end of linked list, so we can't free the chunks.
	 */
	if (prev_head_version != NULL) {
		return;
	}

	atomic_thread_fence(memory_order_seq_cst);

free_head_version:

	chunk = head_version->head_node;
	prev_chunk = chunk;
	while (chunk != head_version->tail_node) {
		chunk = chunk->next;

#ifdef TRCACHE_DEBUG
		pop_head = candle_chunk_index_pop_head(chunk_index);
		assert(pop_head == prev_chunk);
#else  /* !TRCACHE_DEBUG */
		candle_chunk_index_pop_head(chunk_index);
#endif /* TRCACHE_DEBUG */

		if (chunk_list->config->flush_ops.on_batch_destroy != NULL) {
			chunk_list->config->flush_ops.on_batch_destroy(
				prev_chunk->column_batch,
				chunk_list->config->flush_ops.on_destroy_ctx);
		}

		candle_chunk_destroy(prev_chunk);
		prev_chunk = chunk;
	}
#ifdef TRCACHE_DEBUG
	pop_head = candle_chunk_index_pop_head(chunk_index);
	assert(pop_head == head_version->tail_node);
#else  /* !TRCACHE_DEBUG */
	candle_chunk_index_pop_head(chunk_index);
#endif /* TRCACHE_DEBUG */

	if (chunk_list->config->flush_ops.on_batch_destroy != NULL) {
		chunk_list->config->flush_ops.on_batch_destroy(
			head_version->tail_node->column_batch,
			chunk_list->config->flush_ops.on_destroy_ctx);
	}

	candle_chunk_destroy(head_version->tail_node);

	next_head_version = atomic_load(&head_version->head_version_next);

	/*
	 * Recycle or free the head_version.
	 */
	memory_pressure = atomic_load_explicit(
		&trc->mem_acc.memory_pressure, memory_order_acquire);
	
	if (memory_pressure) {
		mem_sub_atomic(&trc->head_version_pool->object_memory_usage.value,
			sizeof(struct candle_chunk_list_head_version));
		free(head_version);
	} else {
		scq_enqueue(trc->head_version_pool, head_version);
	}

	/*
	 * Normally not NULL, but may be NULL if the chunk list is being destroyed.
	 */
	if (next_head_version == NULL) {
		return;
	}

	/*
	 * Try to set the prev pointer of the next head to NULL, so that it
	 * recognizes itself as the end of the list. If this fails, we must free the
	 * nodes of the next head ourselves. Because the thread freeing the next
	 * head might assume it's not the end of the list.
	 */
	prev_head_version = atomic_load(&next_head_version->head_version_prev);
	if (((uint64_t)prev_head_version & HEAD_VERSION_RELEASE_MASK) != 0 ||
		!atomic_compare_exchange_strong(&next_head_version->head_version_prev,
			&prev_head_version, NULL)) {
		/* Release the next head's nodes */
		head_version = next_head_version;
		goto free_head_version;
	}
}

/**
 * @brief   Allocate and initialize the #candle_chunk_list.
 *
 * @param   ctx: Init context that contains parameters.
 *
 * @return  Pointer to candle_chunk_list or NULL on failure.
 */
struct candle_chunk_list *create_candle_chunk_list(
	struct candle_chunk_list_init_ctx *ctx)
{
	struct candle_chunk_list *list = NULL;
	struct atomsnap_init_context atomsnap_ctx = {
		.free_impl = candle_chunk_list_head_free,
		.num_extra_control_blocks = 0
	};

	if (ctx == NULL || ctx->trc == NULL) {
		errmsg(stderr, "Argument is NULL\n");
		return NULL;
	}

	list = aligned_alloc(CACHE_LINE_SIZE, 
		ALIGN_UP(sizeof(struct candle_chunk_list), CACHE_LINE_SIZE));
	if (list == NULL) {
		errmsg(stderr, "#candle_chunk_list allocation failed\n");
		return NULL;
	}

	memset(list, 0, sizeof(struct candle_chunk_list));

	list->trc = ctx->trc;
	list->config = &ctx->trc->candle_configs[ctx->candle_idx];
	list->candle_idx = ctx->candle_idx;
	list->symbol_id = ctx->symbol_id;

	/* Get pointers to the shared, per-candle-type SCQ pools */
	list->chunk_pool = list->trc->chunk_pools[ctx->candle_idx];
	list->row_page_pool = list->trc->row_page_pools[ctx->candle_idx];

	list->chunk_index = candle_chunk_index_create(list->trc,
		list->trc->flush_threshold_pow2, list->trc->batch_candle_count_pow2);
	if (list->chunk_index == NULL) {
		errmsg(stderr, "Failure on candle_chunk_index_create()\n");
		free(list);
		return NULL;
	}

	list->row_page_count = (list->trc->batch_candle_count
		+ TRCACHE_ROWS_PER_PAGE - 1) / TRCACHE_ROWS_PER_PAGE;

	list->head_gate = atomsnap_init_gate(&atomsnap_ctx);
	if (list->head_gate == NULL) {
		errmsg(stderr, "Failure on atomsnap_init_gate()\n");
		candle_chunk_index_destroy(list->chunk_index);
		free(list);
		return NULL;
	}

	atomic_store(&list->mutable_seq, UINT64_MAX);
	atomic_store(&list->last_seq_converted, UINT64_MAX);
	atomic_store(&list->unflushed_batch_count, 0);

	list->tail = NULL;
	list->candle_mutable_chunk = NULL;
	list->converting_chunk = NULL;

	return list;
}

/**
 * @brief   Destroy all chunks.
 *
 * This function must be called only after all chunks in the list have been
 * converted to column batches.
 *
 * @param   chunk_list: Pointer from create_candle_chunk_list().
 */
void destroy_candle_chunk_list(struct candle_chunk_list *chunk_list)
{
	struct atomsnap_version *snap = NULL;
	struct candle_chunk_list_head_version *head_version = NULL;
	struct candle_chunk *chunk = NULL;

	if (chunk_list == NULL) {
		return;
	}

	/* 
	 * Close current head version's tail to free entire chunk list.
	 */
	snap = atomsnap_acquire_version(chunk_list->head_gate);
	if (snap == NULL) {
#ifdef TRCACHE_DEBUG
		errmsg(stderr, "Unused candle chunk list\n");
#endif /* TRCACHE_DEBUG */
		atomsnap_destroy_gate(chunk_list->head_gate);
		candle_chunk_index_destroy(chunk_list->chunk_index);
		free(chunk_list);
		return;
	}

	head_version
		= (struct candle_chunk_list_head_version *)atomsnap_get_object(snap);
	head_version->tail_node = chunk_list->tail;

	/*
	 * Flush all chunks.
	 */
	chunk = head_version->head_node;
	do {
		if (candle_chunk_flush(chunk, &chunk_list->config->flush_ops) == 0) {
			while (candle_chunk_flush_poll(chunk,
					&chunk_list->config->flush_ops) != 1) {
				/* polling */ ;
			}
		}
		chunk = chunk->next;
	} while (chunk != NULL);

	atomsnap_release_version(snap);

	/* 
	 * Force release the current version held by the gate.
	 * This triggers candle_chunk_list_head_free() for the last version.
	 */
	atomsnap_exchange_version(chunk_list->head_gate, NULL);

	atomsnap_destroy_gate(chunk_list->head_gate);

	candle_chunk_index_destroy(chunk_list->chunk_index);

	free(chunk_list);
}

/**
 * @brief   Initialise the very first candle/page of a brand‑new list.
 */
static int init_first_candle(struct candle_chunk_list *list,
	void *trade)
{
	struct candle_chunk *new_chunk = create_candle_chunk(
		list->trc, list->candle_idx, list->symbol_id, list->row_page_count,
		list->trc->batch_candle_count, list->chunk_pool, list->row_page_pool);
	struct atomsnap_version *head_snap_version = NULL;
	struct candle_chunk_list_head_version *head = NULL;
	const struct trcache_candle_update_ops *ops
		= &list->config->update_ops;
	uint64_t first_key;

	if (new_chunk == NULL) {
		errmsg(stderr, "Failure on create_candle_chunk()\n");
		return -1;
	}

	if (candle_chunk_page_init(new_chunk, 0, ops, trade, &first_key) == -1) {
		errmsg(stderr, "Failure on candle_chunk_page_init()\n");
		candle_chunk_destroy(new_chunk);
		return -1;
	}

	head_snap_version = atomsnap_make_version(list->head_gate);
	if (head_snap_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		candle_chunk_destroy(new_chunk);
		return -1;
	}

	head = alloc_head_version_object(list);
	if (head == NULL) {
		errmsg(stderr, "Failure on alloc_head_version_object()\n");
		atomsnap_free_version(head_snap_version);
		candle_chunk_destroy(new_chunk);
		return -1;
	}

	atomsnap_set_object(head_snap_version, head, list);

	head->head_version_prev = NULL;
	head->head_version_next = NULL;
	head->tail_node = NULL;
	head->head_node = new_chunk;

	list->tail = new_chunk;
	list->candle_mutable_chunk = new_chunk;
	list->converting_chunk = new_chunk;

	new_chunk->mutable_page_idx = 0;
	new_chunk->mutable_row_idx = 0;

	new_chunk->seq_first = 0;

	/* Add the new chunk into the index BEFORE register */
	if (candle_chunk_index_append(list->chunk_index, new_chunk,
			new_chunk->seq_first, first_key) == -1) {
		errmsg(stderr, "Failure on candle_chunk_index_append()\n");
		return -1;
	}

	/* Register atomsnap version of head */
	atomsnap_exchange_version(list->head_gate, head_snap_version);

	return 0;
}

/**
 * @brief   Start the next candle within the same page.
 */
static void advance_within_same_page(struct candle_chunk *chunk, 
	struct candle_row_page *row_page,
	const struct trcache_candle_update_ops *ops,
	void *trade)
{
	const struct trcache_candle_config *config
		= &chunk->trc->candle_configs[chunk->column_batch->candle_idx];
	struct trcache_candle_base *next_candle
		= (struct trcache_candle_base *)(row_page->data +
			(++chunk->mutable_row_idx * config->user_candle_size));
	ops->init(next_candle, trade);
	candle_chunk_write_key(chunk,
		chunk->mutable_page_idx, chunk->mutable_row_idx,
		next_candle->key.value);
}

/**
 * @brief   Allocate a new page within the existing chunk and 
 *          start its first candle.
 */
static int advance_to_next_page(struct candle_chunk *chunk,
	const struct trcache_candle_update_ops *ops,
	void *trade)
{
	int new_page_idx = chunk->mutable_page_idx + 1;
	uint64_t dummy;

	if (candle_chunk_page_init(chunk, new_page_idx, ops, trade, &dummy) == -1) {
		errmsg(stderr, "Failure on candle_chunk_page_init()\n");
		return -1;
	}

	chunk->mutable_page_idx = new_page_idx;
	chunk->mutable_row_idx = 0;
	return 0;
}

/**
 * @brief   Allocate a fresh chunk and start its first candle.
 */
static int advance_to_new_chunk(struct candle_chunk_list *list,
	struct candle_chunk *prev_chunk, void *trade)
{
	const struct trcache_candle_update_ops *ops
		= &list->config->update_ops;
	struct candle_chunk *new_chunk = create_candle_chunk(list->trc,
		list->candle_idx, list->symbol_id, list->row_page_count,
		list->trc->batch_candle_count, list->chunk_pool, list->row_page_pool);
	uint64_t first_key;

	if (new_chunk == NULL) {
		errmsg(stderr, "Failure on create_candle_chunk()\n");
		return -1;
	}

	if (candle_chunk_page_init(new_chunk, 0, ops, trade, &first_key) == -1) {
		errmsg(stderr, "Failure on candle_chunk_page_init()\n");
		candle_chunk_destroy(new_chunk);
		return -1;
	}

	/* Link into list */
	prev_chunk->next = new_chunk;
	new_chunk->prev = prev_chunk;
	list->tail = new_chunk;
	list->candle_mutable_chunk = new_chunk;

	new_chunk->mutable_page_idx = 0;
	new_chunk->mutable_row_idx  = 0;
	new_chunk->seq_first = prev_chunk->seq_first + list->trc->batch_candle_count;

	/* Add the new chunk into the index */
	if (candle_chunk_index_append(list->chunk_index, new_chunk,
			new_chunk->seq_first, first_key) == -1) {
		errmsg(stderr, "Failure on candle_chunk_index_append()\n");
		return -1;
	}

	return 0;
}

/**
 * @brief    Apply trade data to the appropriate candle.
 *
 * @param    list:  Pointer to the candle chunk list.
 * @param    trade: Trade data to apply.
 *
 * @return   0 on success, or -1 if the trade data is not applied.
 *
 * Finds the corresponding candle in the chunk list and updates it with the
 * trade. Creates a new chunk if necessary.
 *
 * The admin thread must ensure that the apply function for a single chunk list
 * is executed by only one worker thread at a time.
 */
int candle_chunk_list_apply_trade(struct candle_chunk_list *list,
	void *trade)
{
	struct candle_chunk *chunk = list->candle_mutable_chunk;
	const struct trcache_candle_config *config = list->config;
	const struct trcache_candle_update_ops *ops = &config->update_ops;
	struct atomsnap_version *row_page_version = NULL;
	struct candle_row_page *row_page = NULL;
	struct trcache_candle_base *candle = NULL;
	int expected_num_completed;
	bool consumed;

	/*
	 * This path occurs only once, right after the chunk list is created.
	 * During page initialization, the first candle is also initialized, 
	 * which requires trade data. However, since trade data is not available 
	 * at the time the chunk list is created, the first chunk is created not in
	 * the list init function.
	 */
	if (chunk == NULL) {
		if (init_first_candle(list, trade) == -1) {
			return -1;
		}

		atomic_store_explicit(&list->mutable_seq, 0, memory_order_release);
		return 0;
	}

	row_page_version = atomsnap_acquire_version_slot(chunk->row_gate,
		chunk->mutable_page_idx);
	row_page = (struct candle_row_page *)atomsnap_get_object(row_page_version);
	candle = (struct trcache_candle_base *)(row_page->data +
		(chunk->mutable_row_idx * config->user_candle_size));

	/* 
	 * Attempt to update the latest candle. The update callback returns
	 * true if the trade was consumed by the candle.
	 */
	if (!candle->is_closed) {
		pthread_spin_lock(&chunk->spinlock);
		consumed = ops->update(candle, trade);
		pthread_spin_unlock(&chunk->spinlock);

		if (consumed) {
			atomsnap_release_version(row_page_version);
			return 0;
		}
	}

	expected_num_completed = atomic_load_explicit(&chunk->num_completed, 
		memory_order_acquire) + 1;

	/* 
	 * Move to the next candle and initialize it.
	 * (A) Move the row index within the same page (same chunk).
	 * (B) Move to the next page within the same chunk.
	 * (C) Move to the first page of a new chunk.
	 */
	if (chunk->mutable_row_idx != TRCACHE_ROWS_PER_PAGE - 1 &&
			expected_num_completed != list->trc->batch_candle_count) {
		/* Initialize the next candle */
		advance_within_same_page(chunk, row_page, ops, trade);

		/* Then release the page */
		atomsnap_release_version(row_page_version);
	} else if (chunk->mutable_row_idx == TRCACHE_ROWS_PER_PAGE - 1 &&
			expected_num_completed != list->trc->batch_candle_count) {
		/* First, release the old page */
		atomsnap_release_version(row_page_version);

		/* Then move to the next page */
		if (advance_to_next_page(chunk, ops, trade) == -1) {
			errmsg(stderr, "Failure on advance_to_next_page()\n");
			return -1;
		}
	} else {
		/* First, release the old page */
		atomsnap_release_version(row_page_version);

		/* Then move to the next chunk */
		if (advance_to_new_chunk(list, chunk, trade) == -1) {
			errmsg(stderr, "Failure on advance_to_new_chunk()\n");
			return -1;
		}
	}

	/*
	 * Ensure that the previous mutable candle is completed,
	 * then a new mutable candle can be observed to converter.
	 */
	atomic_store_explicit(&chunk->num_completed, expected_num_completed,
		memory_order_release);
	atomic_store_explicit(&list->mutable_seq, list->mutable_seq + 1,
		memory_order_release);

	return 0;
}

/**
 * @brief    Convert all immutable row candles into a column batch.
 *
 * @param    list: Pointer to the candle chunk list.
 *
 * @return   Number of candles converted in this call.
 *
 * The admin thread must ensure that the convert function for a single chunk
 * list is executed by only one worker thread at a time.
 */
int candle_chunk_list_convert_to_column_batch(struct candle_chunk_list *list)
{
	uint64_t mutable_seq = atomic_load_explicit(
		&list->mutable_seq, memory_order_acquire);
	uint64_t last_seq_converted = atomic_load_explicit(
		&list->last_seq_converted, memory_order_acquire);
	int num_completed, num_converted, start_idx, end_idx, num_flush_batch = 0;
	int total_converted = 0;
	struct candle_chunk *chunk;

	/* No more candles to convert */
	if (mutable_seq == UINT64_MAX || mutable_seq - 1 == last_seq_converted) {
		return 0;
	}

	chunk = list->converting_chunk;

	while (true) {
		assert(chunk != NULL);

		num_completed = atomic_load_explicit(
			&chunk->num_completed, memory_order_acquire);
		num_converted = atomic_load_explicit(
			&chunk->num_converted, memory_order_acquire);

		if (num_completed == num_converted) {
			list->converting_chunk = chunk;
			break;
		}

		start_idx = candle_chunk_calc_record_index(
			chunk->converting_page_idx, chunk->converting_row_idx);
		end_idx = num_completed - 1;

		/* 
		 * Convert row-format candles into the column batch. This function will
		 * modify converting_page_idx, converting_row_idx, and num_converted of
		 * the chunk.
		 */
		candle_chunk_convert_to_batch(chunk, start_idx, end_idx);

		total_converted += (end_idx - start_idx + 1);

		/*
		 * Since the initial value of last_seq_converted is UINT64_MAX, adding
		 * the number of converted candles yields the sequence number of the
		 * last converted candle.
		 */
		last_seq_converted += (end_idx - start_idx + 1);

		/* The chunk is not fully converted, try it again in later */
		if (end_idx != list->trc->batch_candle_count - 1) {
			list->converting_chunk = chunk;
			break;
		}

		/* If this chunk is fully converted, notice it to the flush worker */
		num_flush_batch += 1;
		chunk = chunk->next;
	}

	atomic_store_explicit(&list->last_seq_converted, last_seq_converted,
		memory_order_release);

	atomic_fetch_add_explicit(&list->unflushed_batch_count, num_flush_batch,
		memory_order_release);

	return total_converted;
}

/**
 * @brief    Finalize the current mutable candle and convert all remaining
 *           candles to column format.
 *
 * @param    list: Pointer to the candle chunk list.
 */
void candle_chunk_list_finalize(struct candle_chunk_list *list)
{
	struct candle_chunk *chunk = NULL;
	uint64_t mutable_seq;
	int rec_idx;

	if (list == NULL) {
		return;
	}

	/*
	 * Convert all immutable candles first. The candle currently pointed
	 * to by mutable_seq remains in row form after this call.
	 */
	candle_chunk_list_convert_to_column_batch(list);

	chunk = list->candle_mutable_chunk;
	mutable_seq = atomic_load_explicit(&list->mutable_seq,
		memory_order_acquire);
        
	if (chunk == NULL || mutable_seq == UINT64_MAX) {
		return;
	}

	rec_idx = candle_chunk_calc_record_index(chunk->mutable_page_idx,
		chunk->mutable_row_idx);

	/*
	 * Finalize the last candle and convert it to column format. At destroy
	 * time the candle is effectively immutable, so it is safe to convert.
	 */
	candle_chunk_convert_to_batch(chunk, rec_idx, rec_idx);
}

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list:  Pointer to the candle chunk list.
 *
 * @return   The number of batches flushed in this call.
 *
 * May invoke user-supplied flush callbacks.
 *
 * The admin thread must ensure that the flush function for a single chunk
 * list is executed by only one worker thread at a time.
 */
int candle_chunk_list_flush(struct candle_chunk_list *list)
{
	int flush_batch_count = atomic_load_explicit(&list->unflushed_batch_count,
		memory_order_acquire) - list->trc->flush_threshold;
	int flush_start_count = 0, flush_done_count = 0;
	struct atomsnap_version *head_snap, *new_snap;
	struct candle_chunk_list_head_version *head_version, *new_ver;
	struct candle_chunk *chunk, *last_chunk = NULL;
	const struct trcache_batch_flush_ops *flush_ops
		= &list->config->flush_ops;

	if (flush_batch_count <= 0) {
		return 0;
	}

	new_snap = atomsnap_make_version(list->head_gate);
	if (new_snap == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		return 0;
	}

	new_ver = alloc_head_version_object(list);
	if (new_ver == NULL) {
		errmsg(stderr, "Failure on alloc_head_version_object()\n");
		atomsnap_free_version(new_snap);
		return 0;
	}

	atomsnap_set_object(new_snap, new_ver, list);

	head_snap = atomsnap_acquire_version(list->head_gate);
	head_version = (struct candle_chunk_list_head_version *)
		atomsnap_get_object(head_snap);

	/*
	 * Modifying the head of the list is restricted to a single thread,
	 * coordinated by the admin thread. Thus, only this execution thread is
	 * allowed to update the list head.
	 */
	chunk = head_version->head_node;

	while (chunk != NULL) {
		assert(chunk->num_converted == list->trc->batch_candle_count);

		flush_start_count += 1;

		if (candle_chunk_flush(chunk, flush_ops) == 1) {
			flush_done_count += 1;
		}

		if (flush_start_count == flush_batch_count) {
			last_chunk = chunk;
			break;
		}

		chunk = chunk->next;
	}

	assert(last_chunk != NULL && last_chunk != list->tail);

	/* If asynchronous flush is used, check whether the flush has completed */
	while (flush_done_count < flush_batch_count) {
		chunk = head_version->head_node;
		while (true) {
			if (candle_chunk_flush_poll(chunk, flush_ops) == 1) {
				flush_done_count += 1;
			}
			
			if (chunk == last_chunk) {
				break;
			}
			chunk = chunk->next;
		}
	}

	atomic_fetch_sub_explicit(&list->unflushed_batch_count, flush_done_count,
		memory_order_release);

	/* Change head version */
	new_ver->head_version_prev = head_version;
	new_ver->head_version_next = NULL;

	new_ver->tail_node = NULL;
	new_ver->head_node = last_chunk->next;
	assert(new_ver->head_node != NULL);

	atomic_store(&head_version->head_version_next, new_ver);
	atomic_store(&head_version->tail_node, last_chunk);	

	/*
	 * The above contains essential information for both threads starting
	 * traversal from the head version and for freeing old versions. It must be
	 * set before registering the new head and releasing the old head version.
	 */
	atomsnap_exchange_version(list->head_gate, new_snap);
	atomsnap_release_version(head_snap);

	return flush_done_count;
}

/**
 * @brief   Core implementation shared by copy() functions.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   chunk:       Chunk where the search begins.
 * @param   seq_start:   Minimum sequence number to copy.
 * @param   seq_end:     Maximum sequence number to copy.
 * @param   count:       Number of candles to copy.
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure.
 *
 * @note    head_snap is acquired/released by the caller.
 */
static int candle_chunk_list_copy_backward(
	struct candle_chunk_list *list, struct candle_chunk *chunk,
	uint64_t seq_start, uint64_t seq_end, int count,
	struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	uint64_t mutable_seq, last_converted_seq;
	int start_record_idx, end_record_idx, num_copied, remains = count;

	mutable_seq = atomic_load_explicit(&list->mutable_seq,
		memory_order_acquire);

	if (mutable_seq < seq_end) {
		errmsg(stderr,
			"End sequence number is out of range (seq_end=%" PRIu64 ", "
			"mutable_seq=%" PRIu64 ")\n",
			seq_end, mutable_seq);
		return -1;
	}

	/*
	 * Exceptional case where a spinlock must be acquired.
	 */
	if (seq_end == mutable_seq) {
		end_record_idx = candle_chunk_clamp_seq(chunk, seq_end);
		num_copied = candle_chunk_copy_mutable_row(chunk, end_record_idx,
			remains - 1, dst, request);

		if (num_copied == 1) {
			seq_end -= 1;
			remains -= 1;

			if (remains == 0) {
				dst->num_candles = 1;
				return 0;
			} else if (end_record_idx == 0) {
				chunk = chunk->prev;
			}
		}
	}

	last_converted_seq = atomic_load_explicit(&list->last_seq_converted,
		memory_order_acquire);

	/*
	 * Copy the candles at the chunk level while traversing from the oldest
	 * chunk toward the newest chunk.
	 */
	while (remains > 0) {
		if (last_converted_seq < seq_end || last_converted_seq == UINT64_MAX) {
			start_record_idx = candle_chunk_clamp_seq(chunk, seq_start);
			end_record_idx = candle_chunk_clamp_seq(chunk, seq_end);

			num_copied = candle_chunk_copy_rows_until_converted(chunk,
				start_record_idx, end_record_idx, remains - 1, dst, request);
			seq_end -= num_copied;
			remains -= num_copied;

			if (num_copied == (end_record_idx - start_record_idx + 1)) {
				chunk = chunk->prev;
				continue;
			} else {
				/*
			 	 * If we encounter a crossover point, proceed to the
			 	 * column-batch copy operation below using the same chunk.
			 	 */
				last_converted_seq = atomic_load_explicit(
					&list->last_seq_converted, memory_order_acquire);
			}
		}

		start_record_idx = candle_chunk_clamp_seq(chunk, seq_start);
		end_record_idx = candle_chunk_clamp_seq(chunk, seq_end);

		num_copied = candle_chunk_copy_from_column_batch(chunk,
			start_record_idx, end_record_idx, remains - 1, dst, request);
		seq_end -= num_copied;
		remains -= num_copied;
		chunk = chunk->prev;
	}

	dst->num_candles = count;
	return 0;
}

/**
 * @brief   Copy @count candles ending at @anchor_seq.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   seq_end:     Sequence number of the last candle.
 * @param   count:       Number of candles to copy.
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_list_copy_backward_by_seq(struct candle_chunk_list *list,
	uint64_t seq_end, int count, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	struct candle_chunk_index *idx = list->chunk_index;
	struct candle_chunk *head_chunk, *chunk;
	struct atomsnap_version *head_snap;
	struct candle_chunk_list_head_version *head_ver;
	uint64_t seq_start, head_seq_first, available;
	int ret;

	if (dst == NULL || dst->capacity < count) {
		errmsg(stderr,
			"Invalid #trcache_candle_batch (capacity=%d, requested=%d)\n",
			dst ? dst->capacity : -1, count);
		return -1;
	}

	/* Pin the head of list for safe chunk traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);
	if (head_snap == NULL) {
#ifdef TRCACHE_DEBUG
		errmsg(stderr,
			"Head of candle chunk list is not yet initialized "
			"(seq_end=%" PRIu64 ")(symbol_id=%" PRIu32 ")\n",
			seq_end, list->symbol_id);
#endif /* TRCACHE_DEBUG */
		return -1;
	}
	
	head_ver = (struct candle_chunk_list_head_version *)
		atomsnap_get_object(head_snap);
	head_chunk = head_ver->head_node;

	/*
	 * Search the last chunk FIRST to ensure seq_end is valid and reachable.
	 * If seq_end is found, it guarantees seq_end >= head_chunk->seq_first.
	 */
	chunk = candle_chunk_index_find_seq(idx, seq_end, head_chunk->idx_seq);
	if (chunk == NULL) {
		errmsg(stderr,
			"Target sequence is out of range (seq_end=%" PRIu64 ")\n",
				seq_end);
		atomsnap_release_version(head_snap);
		return -1;
	}

	/* Calculate available candles from the head of the list to seq_end */
	head_seq_first = head_chunk->seq_first;
	available = seq_end - head_seq_first + 1;

	/* If requested count exceeds available history, clamp it */
	if ((uint64_t)count > available) {
		count = (int)available;
	}

	seq_start = seq_end - count + 1;

	ret = candle_chunk_list_copy_backward(list, chunk, seq_start, seq_end,
		count, dst, request);

	atomsnap_release_version(head_snap);
	return ret;
}

/**
 * @brief   Copy @count candles whose range ends at the candle
 *          with the specified @key.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   key:         Key belonging to the last candle.
 * @param   count:       Number of candles to copy.
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure.
 *
 * @note    If @key is outside the range of known candles, returns -1.
 */
int candle_chunk_list_copy_backward_by_key(struct candle_chunk_list *list,
	uint64_t key, int count, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	struct candle_chunk_index *idx = list->chunk_index;
	struct candle_chunk *head_chunk, *chunk;
	struct atomsnap_version *head_snap;
	struct candle_chunk_list_head_version *head_ver;
	uint64_t seq_start, seq_end, head_seq_first, available;
	int ret;

	if (dst == NULL || dst->capacity < count) {
		errmsg(stderr,
			"Invalid #trcache_candle_batch (capacity=%d, requested=%d)\n",
			dst ? dst->capacity : -1, count);
		return -1;
	}

	/* Pin the head of list for safe chunk traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);
	if (head_snap == NULL) {
#ifdef TRCACHE_DEBUG
		errmsg(stderr,
			"Head of candle chunk list is not yet initialized "
			"(key=%" PRIu64 ")\n",
			key);
#endif /* TRCACHE_DEBUG */
		return -1;
	}

	head_ver = (struct candle_chunk_list_head_version *)
		atomsnap_get_object(head_snap);
	head_chunk = head_ver->head_node;

	/* Search the last chunk FIRST */
	chunk = candle_chunk_index_find_key(idx, key, head_chunk->idx_seq);

	if (chunk == NULL) {
		errmsg(stderr,
			"Target key is out of range (key=%" PRIu64 ")\n",
			key);
		atomsnap_release_version(head_snap);
		return -1;
	}

	seq_end = candle_chunk_calc_seq_by_key(chunk, key);

	/* Calculate available candles from the head of the list to seq_end */
	head_seq_first = head_chunk->seq_first;
	available = seq_end - head_seq_first + 1;

	/* If requested count exceeds available history, clamp it */
	if ((uint64_t)count > available) {
		count = (int)available;
	}

	seq_start = seq_end - count + 1;

	ret = candle_chunk_list_copy_backward(list, chunk, seq_start, seq_end,
		count, dst, request);

	atomsnap_release_version(head_snap);
	return ret;
}

/**
 * @brief   Helper to find the sequence number for a key with clamping.
 *
 * Unlike candle_chunk_index_find_key which returns NULL for out-of-range keys,
 * this helper clamps to the available range and returns the corresponding
 * sequence number.
 *
 * @param   idx:          Pointer to the chunk index.
 * @param   target_key:   Key to search for.
 * @param   head_idx_seq: Index sequence of the head chunk.
 * @param   mutable_seq:  Current mutable sequence number.
 * @param   out_chunk:    Output pointer to the found chunk.
 * @param   out_seq:      Output pointer to the sequence number.
 * @param   clamp_low:    If true, clamp to head when key is too low.
 * @param   clamp_high:   If true, clamp to mutable when key is too high.
 *
 * @return  0 on success (exact or clamped), -1 on failure.
 */
static int find_seq_by_key_clamped(struct candle_chunk_index *idx,
	uint64_t target_key, uint64_t head_idx_seq, uint64_t mutable_seq,
	struct candle_chunk **out_chunk, uint64_t *out_seq,
	bool clamp_low, bool clamp_high)
{
	uint64_t head_pos, tail_pos, mask, lo, hi, mid, key_mid;
	uint64_t tail_idx_seq;
	struct atomsnap_version *snap_ver;
	struct candle_chunk_index_version *idx_ver;
	struct candle_chunk *chunk;
	uint64_t head_first_key, tail_last_key;
	int local_idx;

	tail_idx_seq = atomic_load_explicit(&idx->tail, memory_order_acquire);
	snap_ver = atomsnap_acquire_version(idx->gate);
	if (snap_ver == NULL) {
		return -1;
	}

	idx_ver = (struct candle_chunk_index_version *)
		atomsnap_get_object(snap_ver);
	mask = idx_ver->mask;

	head_pos = head_idx_seq & mask;
	tail_pos = tail_idx_seq & mask;

	head_first_key = idx_ver->array[head_pos].key_first;

	/* Get last key from tail chunk's key_array */
	chunk = idx_ver->array[tail_pos].chunk_ptr;
	local_idx = atomic_load_explicit(&chunk->num_completed,
		memory_order_acquire);
	tail_last_key = chunk->column_batch->key_array[local_idx];

	/* Check if target_key is below the available range */
	if (target_key < head_first_key) {
		if (clamp_low) {
			*out_chunk = idx_ver->array[head_pos].chunk_ptr;
			*out_seq = idx_ver->array[head_pos].seq_first;
			atomsnap_release_version(snap_ver);
			return 0;
		}
		atomsnap_release_version(snap_ver);
		return -1;
	}

	/* Check if target_key is above the available range */
	if (target_key > tail_last_key) {
		if (clamp_high) {
			*out_chunk = idx_ver->array[tail_pos].chunk_ptr;
			*out_seq = mutable_seq;
			atomsnap_release_version(snap_ver);
			return 0;
		}
		atomsnap_release_version(snap_ver);
		return -1;
	}

	/* Binary search for the chunk containing target_key */
	lo = head_idx_seq;
	hi = tail_idx_seq;

	while (lo < hi) {
		mid = lo + ((hi - lo + 1) >> 1);
		key_mid = idx_ver->array[mid & mask].key_first;

		if (key_mid <= target_key) {
			lo = mid;
		} else {
			hi = mid - 1;
		}
	}

	chunk = idx_ver->array[lo & mask].chunk_ptr;
	*out_chunk = chunk;

	/* Find exact sequence within the chunk */
	local_idx = candle_chunk_find_idx_by_key(chunk, target_key);
	if (local_idx == -1) {
		/*
		 * Key not found in chunk. Since binary search ensures
		 * chunk.key_first <= target_key, this implies target_key is
		 * beyond the last key of this chunk (Gap After scenario).
		 */
		uint64_t num_completed = atomic_load_explicit(&chunk->num_completed,
			memory_order_acquire);

		if (clamp_high) {
			/*
			 * For end_key: We want the max valid seq <= target_key.
			 * Since target_key is beyond this chunk, the last candle
			 * of this chunk is the closest valid lower bound.
			 */
			if (num_completed > 0) {
				*out_seq = chunk->seq_first + num_completed - 1;
			} else {
				/* Should not happen for valid chunks in index */
				*out_seq = chunk->seq_first;
			}
		} else {
			/*
			 * For start_key: We want the min valid seq >= target_key.
			 * Since target_key is beyond this chunk, the first candle
			 * of the NEXT chunk is the answer.
			 */
			*out_seq = chunk->seq_first + num_completed;
		}
	} else {
		*out_seq = chunk->seq_first + local_idx;
	}

	atomsnap_release_version(snap_ver);
	return 0;
}

/**
 * @brief   Copy candles within the key range [start_key, end_key].
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   start_key:   Key of the first candle (inclusive).
 * @param   end_key:     Key of the last candle (inclusive).
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure (e.g., capacity insufficient).
 *
 * @note    If start_key or end_key is outside the available range, the query
 *          is clamped to the available bounds. If no candles fall within the
 *          range, dst->num_candles is set to 0 and returns 0.
 */
int candle_chunk_list_copy_by_key_range(struct candle_chunk_list *list,
	uint64_t start_key, uint64_t end_key, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request)
{
	struct candle_chunk_index *idx = list->chunk_index;
	struct candle_chunk *head_chunk, *start_chunk, *end_chunk;
	struct atomsnap_version *head_snap;
	struct candle_chunk_list_head_version *head_ver;
	uint64_t seq_start, seq_end, mutable_seq;
	int count, ret;

	if (dst == NULL) {
		errmsg(stderr, "Destination batch is NULL\n");
		return -1;
	}

	/* Handle invalid range: start > end */
	if (start_key > end_key) {
		dst->num_candles = 0;
		return 0;
	}

	/* Pin the head of list for safe chunk traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);
	if (head_snap == NULL) {
		/* List not initialized yet, return empty result */
		dst->num_candles = 0;
		return 0;
	}

	head_ver = (struct candle_chunk_list_head_version *)
		atomsnap_get_object(head_snap);
	head_chunk = head_ver->head_node;

	mutable_seq = atomic_load_explicit(&list->mutable_seq,
		memory_order_acquire);

	/* Handle empty list */
	if (mutable_seq == UINT64_MAX) {
		atomsnap_release_version(head_snap);
		dst->num_candles = 0;
		return 0;
	}

	/* Find start sequence with clamping to head */
	if (find_seq_by_key_clamped(idx, start_key, head_chunk->idx_seq,
			mutable_seq, &start_chunk, &seq_start, true, false) == -1) {
		/* start_key > all available keys */
		atomsnap_release_version(head_snap);
		dst->num_candles = 0;
		return 0;
	}

	/* Find end sequence with clamping to tail */
	if (find_seq_by_key_clamped(idx, end_key, head_chunk->idx_seq,
			mutable_seq, &end_chunk, &seq_end, false, true) == -1) {
		/* end_key < all available keys */
		atomsnap_release_version(head_snap);
		dst->num_candles = 0;
		return 0;
	}

	/* After clamping, check if range is still valid */
	if (seq_start > seq_end) {
		atomsnap_release_version(head_snap);
		dst->num_candles = 0;
		return 0;
	}

	count = (int)(seq_end - seq_start + 1);

	/* Check capacity */
	if (dst->capacity < count) {
		errmsg(stderr,
			"Insufficient capacity (capacity=%d, required=%d)\n",
			dst->capacity, count);
		atomsnap_release_version(head_snap);
		return -1;
	}

	ret = candle_chunk_list_copy_backward(list, end_chunk, seq_start,
		seq_end, count, dst, request);

	atomsnap_release_version(head_snap);
	return ret;
}

/**
 * @brief   Count the number of candles within the key range.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   start_key:   Key of the first candle (inclusive).
 * @param   end_key:     Key of the last candle (inclusive).
 *
 * @return  Number of candles in the range (>= 0), or -1 on failure.
 *
 * @note    If start_key or end_key is outside the available range, the count
 *          is based on the clamped bounds. Returns 0 if no candles exist.
 */
int candle_chunk_list_count_by_key_range(struct candle_chunk_list *list,
	uint64_t start_key, uint64_t end_key)
{
	struct candle_chunk_index *idx = list->chunk_index;
	struct candle_chunk *head_chunk, *start_chunk, *end_chunk;
	struct atomsnap_version *head_snap;
	struct candle_chunk_list_head_version *head_ver;
	uint64_t seq_start, seq_end, mutable_seq;

	/* Handle invalid range: start > end */
	if (start_key > end_key) {
		return 0;
	}

	/* Pin the head of list for safe chunk traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);
	if (head_snap == NULL) {
		/* List not initialized yet */
		return 0;
	}

	head_ver = (struct candle_chunk_list_head_version *)
		atomsnap_get_object(head_snap);
	head_chunk = head_ver->head_node;

	mutable_seq = atomic_load_explicit(&list->mutable_seq,
		memory_order_acquire);

	/* Handle empty list */
	if (mutable_seq == UINT64_MAX) {
		atomsnap_release_version(head_snap);
		return 0;
	}

	/* Find start sequence with clamping to head */
	if (find_seq_by_key_clamped(idx, start_key, head_chunk->idx_seq,
			mutable_seq, &start_chunk, &seq_start, true, false) == -1) {
		/* start_key > all available keys */
		atomsnap_release_version(head_snap);
		return 0;
	}

	/* Find end sequence with clamping to tail */
	if (find_seq_by_key_clamped(idx, end_key, head_chunk->idx_seq,
			mutable_seq, &end_chunk, &seq_end, false, true) == -1) {
		/* end_key < all available keys */
		atomsnap_release_version(head_snap);
		return 0;
	}

	atomsnap_release_version(head_snap);

	/* After clamping, check if range is still valid */
	if (seq_start > seq_end) {
		return 0;
	}

	return (int)(seq_end - seq_start + 1);
}
