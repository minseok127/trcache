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

#include "core/candle_chunk_list.h"
#include "core/trcache_internal.h"

/**
 * @brief   Allocate an candle chunk list's head version.
 *
 * @return  Pointer to #atomsnap_version, or NULL on failure.
 */
static struct atomsnap_version *candle_chunk_list_head_alloc(
	void *unused __attribute__((unused)))
{
	struct atomsnap_version *version = NULL;
	struct candle_chunk_list_head_version *head = NULL;

	version = malloc(sizeof(struct atomsnap_version));

	if (version == NULL) {
		fprintf(stderr, "candle_chunk_list_head_alloc: version alloc failed\n");
		return NULL;
	}

	head = malloc(sizeof(struct candle_chunk_list_head_version));

	if (head == NULL) {
		fprintf(stderr, "candle_chunk_list_head_alloc: head alloc failed\n");
		free(version);
		return NULL;
	}

	head->snap_version = version;
	version->object = (void *)head;

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
	struct candle_chunk_list_head_version *head_version
		= (struct candle_chunk_list_head_version *)version->object;
	struct candle_chunk_list_head_version *next_head_version = NULL;
	struct candle_chunk_list_head_version *prev_head_version = NULL;
	struct candle_chunk *prev_node = NULL, *node = NULL;

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

	node = head_version->head_node;
	prev_node = node;
	while (node != head_version->tail_node) {
		node = node->next;
		// XXX remove it from candle chunk index
		candle_chunk_destroy(prev_node);
		prev_node = node;
	}
	// XXX remove it from candle chunk index
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
	struct atomsnap_version *head_snap_version = NULL;
	struct candle_chunk_list_head_version *head = NULL;
	struct candle_chunk *chunk = NULL;
	struct atomsnap_init_context atomsnap_ctx = {
		.atomsnap_alloc_impl = candle_chunk_list_head_alloc,
		.atomsnap_free_impl = candle_chunk_list_head_free,
		.num_extra_control_blocks = 0
	};

	if (ctx == NULL || ctx->trc == NULL) {
		fprintf(stderr, "create_candle_chunk_list: ctx error\n");
		return NULL;
	}

	list = malloc(sizeof(struct candle_chunk_list));
	if (list == NULL) {
		fprintf(stderr, "create_candle_chunk_list: list alloc failed\n");
		return NULL;
	}

	list->trc = ctx->trc;

	list->row_page_count = (ctx->trc->batch_candle_count
		+ TRCACHE_ROWS_PER_PAGE - 1) / TRCACHE_ROWS_PER_PAGE;

	chunk = create_candle_chunk(list->row_page_count);
	if (chunk == NULL) {
		fprintf(stderr, "create_candle_chunk_list: create chunk failed\n");
		free(list);
		return NULL;
	}

	list->head_gate = atomsnap_init_gate(&atomsnap_ctx);
	if (list->head_gate == NULL) {
		fprintf(stderr, "create_candle_chunk_list: atomsnap_init failed\n");
		free(list);
		free(chunk);
		return NULL;
	}

	head_snap_version = atomsnap_make_version(list->head_gate, NULL);
	if (head_snap_version == NULL) {
		fprintf(stderr, "create_candle_chunk_list: make version failed\n");
		atomsnap_destroy_gate(list->head_gate);
		free(list);
		free(chunk);
		return NULL;
	}

	head = (struct candle_chunk_list_head_version *)head_snap_version->object;
	head->head_version_prev = NULL;
	head->head_version_next = NULL;
	head->tail_node = NULL;  /* Node range is not yet closed */
	head->head_node = chunk; /* First node of the range */

	atomic_store(&list->mutable_seq, UINT64_MAX);
	atomic_store(&list->last_seq_converted, UINT64_MAX);
	atomic_store(&list->unflushed_batch_count, 0);

	list->update_ops = ctx->update_ops;
	list->candle_type = ctx->candle_type;
	list->symbol_id = ctx->symbol_id;

	list->tail = chunk;
	list->candle_mutable_chunk = chunk;
	list->converting_chunk = chunk;

	return list;
}

/**
 * @brief   Destroy all chunks.
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

	free(chunk_list);
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
	struct candle_chunk *chunk = list->candle_mutable_chunk, *new_chunk = NULL;
	struct candle_update_ops *ops = &list->update_ops;
	struct atomsnap_version *row_page_version = NULL;
	struct candle_row_page *row_page = NULL;
	struct trcache_candle *candle = NULL;
	int expected_num_completed = 1 + 
		atomic_load_explicit(&chunk->num_completed, memory_order_acquire);

	/*
	 * This path occurs only once, right after the chunk list is created.
	 * During page initialization, the first candle is also initialized, 
	 * which requires trade data. However, since trade data is not available 
	 * at the time the chunk list is created, this separate path was introduced.
	 */
	if (chunk->mutable_page_idx == -1) {
		if (candle_chunk_page_init(chunk, 0, ops, trade) == -1) {
			fprintf(stderr, "candle_chunk_list_apply_trade: init page failed\n");
			return -1; /* trade data is not applied */
		}

		chunk->mutable_page_idx = 0;
		chunk->mutable_row_idx = 0;

		// XXX candle chunk index insert

		/* Now the new candle visible to readers */
		atomic_store(&list->mutable_seq, 0);
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

	/* 
	 * Move to the next candle and initialize it.
	 * (A) Move the row index within the same page (same chunk).
	 * (B) Move to the next page within the same chunk.
	 * (C) Move to the first page of a new chunk.
	 */
	if (chunk->mutable_row_idx != TRCACHE_ROWS_PER_PAGE - 1 &&
			expected_num_completed != list->trc->batch_candle_count) {
		chunk->mutable_row_idx += 1;
		candle = &(row_page->rows[chunk->mutable_row_idx]);

		/*
	 	 * Since the completion count has not been incremented yet, the candle
	 	 * initialization process is not visible to readers. So no locking is
	 	 * required.
	 	 */
		ops->init(candle, trade);
		atomsnap_release_version(row_page_version);
	} else if (chunk->mutable_row_idx == TRCACHE_ROWS_PER_PAGE - 1 &&
			expected_num_completed != list->trc->batch_candle_count) {
		/* Release old page */
		atomsnap_release_version(row_page_version);

		chunk->mutable_page_idx += 1;
		if (candle_chunk_page_init(
				chunk, chunk->mutable_page_idx, ops, trade) == -1) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: new page init failed\n");
			chunk->mutable_page_idx -= 1; /* rollback */
			return -1; /* trade data is not applied */
		}

		chunk->mutable_row_idx = 0;
	} else {
		/* Release old page */
		atomsnap_release_version(row_page_version);

		new_chunk = create_candle_chunk(list->row_page_count);
		if (new_chunk == NULL) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: invalid new chunk\n");
			return -1; /* trade data is not applied */
		}

		if (candle_chunk_page_init(new_chunk, 0, ops, trade) == -1) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: new chunk init failed\n");
			candle_chunk_destroy(new_chunk);
			return -1; /* trade data is not applied */
		}

		new_chunk->mutable_page_idx = 0;
		new_chunk->mutable_row_idx = 0;
		list->candle_mutable_chunk = new_chunk;

		/* Add the new chunk into tail of the linked list */
		chunk->next = new_chunk;
		list->tail = new_chunk;

		// XXX candle chunk index old chunk update
		// XXX candle chunk index insert
	}

	/*
	 * Ensure that the previous mutable candle is completed, and a new mutable
	 * candle can be observed outside the module.
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
	struct candle_chunk *chunk = list->converting_chunk;
	uint64_t mutable_seq = atomic_load_explicit(
		&list->mutable_seq, memory_order_acquire);
	uint64_t last_seq_converted = atomic_load_explicit(
		&list->last_seq_converted, memory_order_acquire);
	struct atomsnap_version *head_snap = NULL;
	int num_completed, num_converted, start_idx, end_idx;
	int num_flush_batch = 0;

	/* No more candles to convert */
	if (mutable_seq == UINT64_MAX || mutable_seq - 1 == last_seq_converted) {
		return;
	}

	/* Pin the head */
	head_snap = atomsnap_acquire_version(list->head_gate);

	while (chunk != NULL) {
		num_completed = atomic_load_explicit(
			&chunk->num_completed, memory_order_acquire);
		num_converted = atomic_load_explicit(
			&chunk->num_converted, memory_order_acquire);

		if (num_completed == num_converted) {
			list->converting_chunk = chunk;
			break;
		}

		/*
		 * If conversion is being started for the first time in this chunk,
		 * allocate and initialze column-batch.
		 */
		if (chunk->column_batch == NULL) {
			chunk->column_batch
				= trcache_batch_alloc_on_heap(list->trc->batch_candle_count);
			if (chunk->column_batch == NULL) {
				fprintf(stderr,
					"candle_chunk_list_convert_to_column_batch: batch err\n");
				break;
			}

			chunk->column_batch->symbol_id = list->symbol_id;
			chunk->column_batch->candle_type = list->candle_type;
			chunk->converting_page_idx = 0;
			chunk->converting_row_idx = 0;
		}

		/* Convert row-format candles into the column batch */
		start_idx = candle_chunk_calc_record_index(
			chunk->converting_page_idx, chunk->converting_row_idx);
		end_idx = num_completed - 1;
		candle_chunk_convert_to_batch(chunk, start_idx, end_idx);
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

	assert(chunk != NULL);

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
		memory_order_acquire) - list->trc->flush_threshold_batches;
	int flush_start_count = 0, flush_done_count = 0;
	struct atomsnap_version *head_snap, *new_snap;
	struct candle_chunk_list_head_version *head_version, *new_ver;
	struct candle_chunk *chunk, *last_chunk = NULL;

	if (flush_batch_count < 0) {
		return;
	}

	new_snap = atomsnap_make_version(list->head_gate, NULL);
	if (new_snap == NULL) {
		fprintf(stderr, "candle_chunk_list_flush: new_snap err\n");
		return;
	}

	head_snap = atomsnap_acquire_version(list->head_gate);
	head_version = (struct candle_chunk_list_head_version *)head_snap->object;

	/*
	 * Modifying the head of the list is restricted to a single thread,
	 * coordinated by the admin thread. Thus, only this execution path is
	 * allowed to update the list head, making this head version the latest one.
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
