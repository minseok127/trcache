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

#include "meta/trcache_internal.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/log.h"

/**
 * @brief   Allocate an candle chunk list's head version.
 *
 * @param   chunk_index: #candle_chunk_index pointer.
 *
 * @return  Pointer to #atomsnap_version, or NULL on failure.
 */
static struct atomsnap_version *candle_chunk_list_head_alloc(
	void *chunk_index)
{
	struct atomsnap_version *version = NULL;
	struct candle_chunk_list_head_version *head = NULL;

	version = malloc(sizeof(struct atomsnap_version));

	if (version == NULL) {
		errmsg(stderr, "#atomsnap_version allocation failed\n");
		return NULL;
	}

	head = malloc(sizeof(struct candle_chunk_list_head_version));

	if (head == NULL) {
		errmsg(stderr, "#candle_chunk_list_head_version allocation failed\n");
		free(version);
		return NULL;
	}

	head->snap_version = version;
	version->object = (void *)head;
	version->free_context = chunk_index;

	return version;
}

#define HEAD_VERSION_RELEASE_MASK (0x8000000000000000ULL)

/**
 * @brief   Frees the chunks covered by the given head version.
 *
 * @param   version: Version holding the #candle_chunk_list_head_version.
 */
static void candle_chunk_list_head_free(struct atomsnap_version *version)
{
	struct candle_chunk_index *chunk_index
		= (struct candle_chunk_index *)version->free_context;
	struct candle_chunk_list_head_version *head_version
		= (struct candle_chunk_list_head_version *)version->object;
	struct candle_chunk_list_head_version *next_head_version = NULL;
	struct candle_chunk_list_head_version *prev_head_version = NULL;
	struct candle_chunk *prev_chunk = NULL, *chunk = NULL;
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

		candle_chunk_destroy(prev_chunk);
		prev_chunk = chunk;
	}
#ifdef TRCACHE_DEBUG
	pop_head = candle_chunk_index_pop_head(chunk_index);
	assert(pop_head == head_version->tail_node);
#else  /* !TRCACHE_DEBUG */
	candle_chunk_index_pop_head(chunk_index);
#endif /* TRCACHE_DEBUG */
	candle_chunk_destroy(head_version->tail_node);

	next_head_version = atomic_load(&head_version->head_version_next);

	free(head_version->snap_version); /* #atomsnap_version */
	free(head_version); /* #candle_chunk_list_head_version */

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
		!atomic_compare_exchange_weak(&next_head_version->head_version_prev,
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
		.atomsnap_alloc_impl = candle_chunk_list_head_alloc,
		.atomsnap_free_impl = candle_chunk_list_head_free,
		.num_extra_control_blocks = 0
	};

	if (ctx == NULL || ctx->trc == NULL) {
		errmsg(stderr, "Argument is NULL\n");
		return NULL;
	}

	list = malloc(sizeof(struct candle_chunk_list));
	if (list == NULL) {
		errmsg(stderr, "#candle_chunk_list allocation failed\n");
		return NULL;
	}

	list->trc = ctx->trc;

	list->chunk_index = candle_chunk_index_create(
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

	list->update_ops = ctx->update_ops;
	list->candle_type = ctx->candle_type;
	list->symbol_id = ctx->symbol_id;

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
	head_version = (struct candle_chunk_list_head_version *)snap->object;
	head_version->tail_node = chunk_list->tail;

	/*
	 * Flush all chunks.
	 */
	chunk = head_version->head_node;
	do {
		if (candle_chunk_flush(chunk_list->trc, chunk) == 0) {
			while (candle_chunk_flush_poll(chunk_list->trc, chunk) != 1) {
				/* polling */ ;
			}
		}
		chunk = chunk->next;
	} while (chunk != NULL);

	atomsnap_release_version(snap);

	/* This will call candle_chunk_list_head_free() */
	atomsnap_destroy_gate(chunk_list->head_gate);

	candle_chunk_index_destroy(chunk_list->chunk_index);

	free(chunk_list);
}

/**
 * @brief   Initialise the very first candle/page of a brand‑new list.
 */
static int init_first_candle(struct candle_chunk_list *list,
	const struct candle_update_ops *ops, struct trcache_trade_data *trade)
{
	struct candle_chunk *new_chunk = create_candle_chunk(list->candle_type,
		list->symbol_id, list->row_page_count, list->trc->batch_candle_count);
	struct atomsnap_version *head_snap_version = NULL;
	struct candle_chunk_list_head_version *head = NULL;

	if (new_chunk == NULL) {
		errmsg(stderr, "Failure on create_candle_chunk()\n");
		return -1;
	}

	if (candle_chunk_page_init(new_chunk, 0, ops, trade) == -1) {
		errmsg(stderr, "Failure on candle_chunk_page_init()\n");
		candle_chunk_destroy(new_chunk);
		return -1;
	}

	head_snap_version
		= atomsnap_make_version(list->head_gate, (void *)list->chunk_index);
	if (head_snap_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		candle_chunk_destroy(new_chunk);
		return -1;
	}

	head = (struct candle_chunk_list_head_version *)head_snap_version->object;
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

	/* Register atomsnap version of head */
	atomsnap_exchange_version(list->head_gate, head_snap_version);

	/* Add the new chunk into the index */
	if (candle_chunk_index_append(list->chunk_index, new_chunk,
			new_chunk->seq_first, trade->timestamp) == -1) {
		errmsg(stderr, "Failure on candle_chunk_index_append()\n");
		return -1;
	}

	return 0;
}

/**
 * @brief   Start the next candle within the same page.
 */
static void advance_within_same_page(struct candle_chunk *chunk, 
	struct candle_row_page *row_page, const struct candle_update_ops *ops,
	struct trcache_trade_data *trade)
{
	ops->init(&row_page->rows[++chunk->mutable_row_idx], trade);
	candle_chunk_write_start_timestamp(chunk, chunk->mutable_page_idx,
		chunk->mutable_row_idx, trade->timestamp);
}

/**
 * @brief   Allocate a new page within the existing chunk and 
 *          start its first candle.
 */
static int advance_to_next_page(struct candle_chunk *chunk,
	const struct candle_update_ops *ops, struct trcache_trade_data *trade)
{
	int new_page_idx = chunk->mutable_page_idx + 1;

	if (candle_chunk_page_init(chunk, new_page_idx, ops, trade) == -1) {
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
	struct candle_chunk *prev_chunk, const struct candle_update_ops *ops,
	struct trcache_trade_data *trade)
{
	struct candle_chunk *new_chunk = create_candle_chunk(list->candle_type,
		list->symbol_id, list->row_page_count, list->trc->batch_candle_count);

	if (new_chunk == NULL) {
		errmsg(stderr, "Failure on create_candle_chunk()\n");
		return -1;
	}

	if (candle_chunk_page_init(new_chunk, 0, ops, trade) == -1) {
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
			new_chunk->seq_first, trade->timestamp) == -1) {
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
	struct trcache_trade_data *trade)
{
	struct candle_chunk *chunk = list->candle_mutable_chunk;
	const struct candle_update_ops *ops = list->update_ops;
	struct atomsnap_version *row_page_version = NULL;
	struct candle_row_page *row_page = NULL;
	struct trcache_candle *candle = NULL;
	int expected_num_completed;

	/*
	 * This path occurs only once, right after the chunk list is created.
	 * During page initialization, the first candle is also initialized, 
	 * which requires trade data. However, since trade data is not available 
	 * at the time the chunk list is created, the first chunk is created not in
	 * the list init function.
	 */
	if (chunk == NULL) {
		if (init_first_candle(list, ops, trade) == -1) {
			return -1;
		}

		atomic_store_explicit(&list->mutable_seq, 0, memory_order_release);
		return 0;
	}

	row_page_version = atomsnap_acquire_version_slot(chunk->row_gate,
		chunk->mutable_page_idx);
	row_page = (struct candle_row_page *)row_page_version->object;
	candle = &(row_page->rows[chunk->mutable_row_idx]);

	/*
	 * For time-based candles, their completion cannot be determined by the
	 * candle alone. Instead, completion is determined only when a new trade
	 * data arrives by checking whether the new data falls within the time range
	 * of the current candle.
	 */
	if (!ops->is_closed(candle, trade)) {
		/* Protect from readers */
		pthread_spin_lock(&chunk->spinlock);
		ops->update(candle, trade);
		pthread_spin_unlock(&chunk->spinlock);
		atomsnap_release_version(row_page_version);
		return 0;
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
		if (advance_to_new_chunk(list, chunk, ops, trade) == -1) {
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
 * The admin thread must ensure that the convert function for a single chunk
 * list is executed by only one worker thread at a time.
 */
void candle_chunk_list_convert_to_column_batch(struct candle_chunk_list *list)
{
	uint64_t mutable_seq = atomic_load_explicit(
		&list->mutable_seq, memory_order_acquire);
	uint64_t last_seq_converted = atomic_load_explicit(
		&list->last_seq_converted, memory_order_acquire);
	struct candle_chunk *chunk = list->converting_chunk;
	struct atomsnap_version *head_snap = NULL;
	int num_completed, num_converted, start_idx, end_idx;
	int num_flush_batch = 0;

	/* No more candles to convert */
	if (mutable_seq == UINT64_MAX || mutable_seq - 1 == last_seq_converted) {
		return;
	}

	/* Pin the head for safe node traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);

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

	/* Unpin head */
	atomsnap_release_version(head_snap);

	atomic_store_explicit(&list->last_seq_converted, last_seq_converted,
		memory_order_release);

	atomic_fetch_add_explicit(&list->unflushed_batch_count, num_flush_batch,
		memory_order_release);
}

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list:  Pointer to the candle chunk list.
 *
 * May invoke user-supplied flush callbacks.
 *
 * The admin thread must ensure that the flush function for a single chunk
 * list is executed by only one worker thread at a time.
 */
void candle_chunk_list_flush(struct candle_chunk_list *list)
{
	int flush_batch_count = atomic_load_explicit(&list->unflushed_batch_count,
		memory_order_acquire) - list->trc->flush_threshold;
	int flush_start_count = 0, flush_done_count = 0;
	struct atomsnap_version *head_snap, *new_snap;
	struct candle_chunk_list_head_version *head_version, *new_ver;
	struct candle_chunk *chunk, *last_chunk = NULL;

	if (flush_batch_count <= 0) {
		return;
	}

	new_snap = atomsnap_make_version(list->head_gate, (void*)list->chunk_index);
	if (new_snap == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		return;
	}

	head_snap = atomsnap_acquire_version(list->head_gate);
	head_version = (struct candle_chunk_list_head_version *)head_snap->object;

	/*
	 * Modifying the head of the list is restricted to a single thread,
	 * coordinated by the admin thread. Thus, only this execution thread is
	 * allowed to update the list head.
	 */
	chunk = head_version->head_node;

	while (chunk != NULL) {
		assert(chunk->num_converted == list->trc->batch_candle_count);

		flush_start_count += 1;

		if (candle_chunk_flush(list->trc, chunk) == 1) {
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
		do {
			if (candle_chunk_flush_poll(list->trc, chunk) == 1) {
				flush_done_count += 1;
			}
			chunk = chunk->next;
		} while (chunk != last_chunk);
	}

	atomic_fetch_sub_explicit(&list->unflushed_batch_count, flush_done_count,
		memory_order_release);

	/* Change head version */
	new_ver = (struct candle_chunk_list_head_version *)new_snap->object;

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
 * @param   field_mask:  Bitmask representing trcache_candle_field_type flags.
 *
 * @return  0 on success, -1 on failure.
 *
 * @note    head_snap is acquired/released by the caller.
 */
static int candle_chunk_list_copy_backward(
	struct candle_chunk_list *list, struct candle_chunk *chunk,
	uint64_t seq_start, uint64_t seq_end, int count,
	struct trcache_candle_batch *dst, trcache_candle_field_flags field_mask)
{
	uint64_t mutable_seq, last_converted_seq;
	int start_record_idx, end_record_idx, num_copied, remains = count;

	mutable_seq = atomic_load_explicit(&list->mutable_seq,
		memory_order_acquire);

	if (mutable_seq < seq_end) {
		errmsg(stderr, "End sequence number is out of range\n");
		return -1;
	}

	/*
	 * Exceptional case where a spinlock must be acquired.
	 */
	if (seq_end == mutable_seq) {
		end_record_idx = candle_chunk_clamp_seq(chunk, seq_end);
		num_copied = candle_chunk_copy_mutable_row(chunk, end_record_idx,
			remains - 1, dst, field_mask);

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
		if (last_converted_seq < seq_end) {
			start_record_idx = candle_chunk_clamp_seq(chunk, seq_start);
			end_record_idx = candle_chunk_clamp_seq(chunk, seq_end);

			num_copied = candle_chunk_copy_rows_until_converted(chunk,
				start_record_idx, end_record_idx, remains - 1, dst, field_mask);
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
			start_record_idx, end_record_idx, remains - 1, dst, field_mask);
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
 * @param   field_mask:  Bitmask representing trcache_candle_field_type flags.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_list_copy_backward_by_seq(struct candle_chunk_list *list,
	uint64_t seq_end, int count, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask)
{
	struct candle_chunk_index *idx = list->chunk_index;
	struct candle_chunk *head_chunk, *chunk;
	struct atomsnap_version *head_snap;
	struct candle_chunk_list_head_version *head_ver;
	uint64_t seq_start = seq_end - count + 1;
	int ret;

	if (dst == NULL || dst->capacity < count) {
		errmsg(stderr, "Invalid #trcache_candle_batch\n");
		return -1;
	}

	/* Pin the head of list for safe chunk traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);
	head_ver = (struct candle_chunk_list_head_version *)head_snap->object;
	head_chunk = head_ver->head_node;

	if (seq_start < head_chunk->seq_first) {
		errmsg(stderr, "Start sequence number is out of range\n");
		atomsnap_release_version(head_snap);
		return -1;
	}

	/* Search the last chunk after pinning the head */
	chunk = candle_chunk_index_find_seq(idx, seq_end);
	assert(chunk != NULL);

	ret = candle_chunk_list_copy_backward(list, chunk, seq_start, seq_end,
		count, dst, field_mask);

	atomsnap_release_version(head_snap);
	return ret;
}

/**
 * @brief   Copy @count candles whose range ends at the candle
 *          that contains @ts_end.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   ts_end:      Timestamp belonging to the last candle.
 * @param   count:       Number of candles to copy.
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   field_mask:  Bitmask representing trcache_candle_field_type flags.
 *
 * @return  0 on success, -1 on failure.
 *
 * @note    If @ts_end is greater than the start timestamp of the most recent
 *          candle, return -1 without identifying a containing candle.
 */
int candle_chunk_list_copy_backward_by_ts(struct candle_chunk_list *list,
	uint64_t ts_end, int count, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask)
{
	struct candle_chunk_index *idx = list->chunk_index;
	struct candle_chunk *head_chunk, *chunk;
	struct atomsnap_version *head_snap;
	struct candle_chunk_list_head_version *head_ver;
	uint64_t seq_start, seq_end;
	int ret;

	if (dst == NULL || dst->capacity < count) {
		errmsg(stderr, "Invalid #trcache_candle_batch\n");
		return -1;
	}

	/* Pin the head of list for safe chunk traversing */
	head_snap = atomsnap_acquire_version(list->head_gate);
	head_ver = (struct candle_chunk_list_head_version *)head_snap->object;
	head_chunk = head_ver->head_node;

	/* Search the last chunk after pinning the head */
	chunk = candle_chunk_index_find_ts(idx, ts_end);

	if (chunk == NULL) {
		errmsg(stderr, "Target timestamp is out of range\n");
		atomsnap_release_version(head_snap);
		return -1;
	}

	seq_end = candle_chunk_calc_seq_by_ts(chunk, ts_end);
	seq_start = seq_end - count + 1;
	assert(seq_end != UINT64_MAX);

	if (seq_start < head_chunk->seq_first) {
		errmsg(stderr, "Start sequence number is out of range\n");
		atomsnap_release_version(head_snap);
		return -1;
	}

	ret = candle_chunk_list_copy_backward(list, chunk, seq_start, seq_end,
		count, dst, field_mask);

	atomsnap_release_version(head_snap);
	return ret;
}
