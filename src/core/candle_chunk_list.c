/* 
 * @file   candle_chunk_list.c
 * @brief  Implementation of row -> column staging chunk for trcache.
 *
 * This module manages one candle chunk, which buffers real‑time trades into
 * row‑form pages, converts them to column batches, and finally flushes the
 * result. All shared structures are protected by atomsnap gates.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>

#include "core/candle_chunk_list.h"
#include "trcache.h"

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
		fprintf(stderr, "row_page_version_alloc: page alloc failed\n");
		return NULL;
	} else {
		memset(row_page, 0, sizeof(struct candle_row_page));
	}

	version = malloc(sizeof(struct atomsnap_version));
	if (version == NULL) {
		fprintf(stderr, "row_page_version_alloc: version alloc failed\n");
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
 * @param   row_page_count:     Number of row pages per chunk.
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
static struct candle_chunk *create_candle_chunk(uint32_t row_page_count)
{
	struct candle_chunk *chunk = NULL;
	struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = row_page_version_alloc,
		.atomsnap_free_impl = row_page_version_free,
		.num_extra_control_blocks = row_page_count - 1
	};

	chunk = malloc(sizeof(struct candle_chunk));
	if (chunk == NULL) {
		fprintf(stderr, "create_candle_chunk: chunk alloc failed\n");
		return NULL;
	}

	if (pthread_spin_init(&chunk->spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
		fprintf(stderr, "create_candle_chunk: spinlock init failed\n");
		free(chunk);
		return NULL;
	}

	chunk->row_gate = atomsnap_init_gate(&ctx);
	if (chunk->row_gate == NULL) {
		fprintf(stderr, "create_candle_chunk: atomsnap_init failed\n");
		pthread_spin_destroy(&chunk->spinlock);
		free(chunk);
		return NULL;
	}

	chunk->column_batch = NULL; /* lazy allocation */
	chunk->mutable_page_idx = -1;
	chunk->mutable_row_idx = -1;
	chunk->converting_page_idx = -1;
	chunk->converting_row_idx = -1;

	atomic_store(&chunk->num_completed, 0);
	atomic_store(&chunk->num_converted, 0);

	chunk->next = NULL;

	return chunk;
}

/**
 * @brief   Release all resources of a candle chunk.
 *
 * @param   chunk: Candle-chunk pointer.
 */
static void candle_chunk_destroy(struct candle_chunk *chunk)
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

	__sync_synchronize();

free_head_version:

	node = head_version->head_node;
	prev_node = node;
	while (node != head_version->tail_node) {
		node = node->next;
		candle_chunk_destroy(prev_node);
		prev_node = node;
	}
	candle_chunk_destroy(head_version->tail_node);

	next_head_version = head_version->head_version_next;

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
	 * nodes of the next head ourselves.
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

	list = malloc(sizeof(struct candle_chunk_list));
	if (list == NULL) {
		fprintf(stderr, "create_candle_chunk_list: list alloc failed\n");
		return NULL;
	}

	list->batch_candle_count = ctx->batch_candle_count;
	list->row_page_count
		= (list->batch_candle_count + TRCACHE_ROWS_PER_PAGE - 1)
			/ TRCACHE_ROWS_PER_PAGE;

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
	list->flush_ops = ctx->flush_ops;
	list->flush_threshold_batches = ctx->flush_threshold_batches;
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

	if (chunk_list == NULL) {
		return;
	}

	/* 
	 * Close current head version's tail to free entire chunk list.
	 */
	snap = atomsnap_acquire_version(chunk_list->head_gate);
	head_version = (struct candle_chunk_list_head_version *)snap->object;
	head_version->tail_node = chunk_list->tail;
	atomsnap_release_version(snap);

	/* This will call candle_chunk_list_head_free() */
	atomsnap_destroy_gate(chunk_list->head_gate);

	free(chunk_list);
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
static int candle_page_init(struct candle_chunk *chunk, int page_idx,
	struct candle_update_ops *ops, struct trcache_trade_data *trade)
{
	struct candle_row_page *row_page = NULL;
	struct atomsnap_version *row_page_version
		= atomsnap_make_version(chunk->row_gate, NULL);

	if (row_page_version == NULL) {
		fprintf(stderr, "candle_page_init: version alloc failed\n");
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
	int expected_num_completed = atomic_load(&chunk->num_completed) + 1;

	/*
	 * This path occurs only once, right after the chunk list is created.
	 * During page initialization, the first candle is also initialized, 
	 * which requires trade data. However, since trade data is not available 
	 * at the time the chunk list is created, this separate path was introduced.
	 */
	if (chunk->mutable_page_idx == -1) {
		if (candle_page_init(chunk, 0, ops, trade) == -1) {
			fprintf(stderr, "candle_chunk_list_apply_trade: init page failed\n");
			return -1; /* trade data is not applied */
		}

		chunk->mutable_page_idx = 0;
		chunk->mutable_row_idx = 0;

		/* XXX candle map insert => user can see this candle */

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
			expected_num_completed != list->batch_candle_count) {
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
			expected_num_completed != list->batch_candle_count) {
		/* Release old page */
		atomsnap_release_version(row_page_version);

		chunk->mutable_page_idx += 1;
		if (candle_page_init(
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

		if (candle_page_init(new_chunk, 0, ops, trade) == -1) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: new chunk init failed\n");
			candle_chunk_destroy(new_chunk);
			return -1; /* trade data is not applied */
		}

		new_chunk->mutable_page_idx = 0;
		new_chunk->mutable_row_idx = 0;
		list->candle_mutable_chunk = new_chunk;

		/* Add the new chunk into tail of the linked list */
		atomic_store(&chunk->next, new_chunk);
		atomic_store(&list->tail, new_chunk);
	}

	/* XXX candle map insert => user can see this candle */

	/*
	 * Ensure that the previous mutable candle is completed, and a new mutable
	 * candle can be observed outside the module.
	 */
	atomic_store(&chunk->num_completed, expected_num_completed);
	atomic_store(&list->mutable_seq, list->mutable_seq + 1);

	return 0;
}

/**
 * @brief   Convert all immutable row candles within the given chunk.
 *
 * @param   chunk:     Target chunk to convert.
 * @param   start_idx: Start record index to convert.
 * @param   end_idx:   End record index to convert.
 */
static void candle_chunk_convert_to_batch(struct candle_chunk *chunk,
	int start_idx, int end_idx)
{
	struct trcache_candle_batch *batch = chunk->column_batch;
	int cur_page_idx = chunk->converting_page_idx, next_page_idx = -1;
	struct atomsnap_version *page_version
		= atomsnap_acquire_version_slot(chunk->row_gate, cur_page_idx);
	struct candle_row_page *page
		= (struct candle_row_page *)page_version->object;
	struct trcache_candle *c = NULL;

	/* Vector pointers */
	uint64_t *ts_ptr = batch->start_timestamp_array + start_idx;
	uint64_t *id_ptr = batch->start_trade_id_array + start_idx;
	uint32_t *ti_ptr = batch->timestamp_interval_array + start_idx;
	uint32_t *ii_ptr = batch->trade_id_interval_array + start_idx;
	double *o_ptr = batch->open_array + start_idx;
	double *h_ptr = batch->high_array + start_idx;
	double *l_ptr = batch->low_array + start_idx;
	double *c_ptr = batch->close_array + start_idx;
	double *v_ptr = batch->volume_array + start_idx;

	for (int idx = start_idx; idx <= end_idx; idx++) {
		next_page_idx = candle_chunk_calc_page_idx(idx);
		if (next_page_idx != cur_page_idx) {
			/* Page is fully converted. Trigger grace period */
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
		*ti_ptr++ = c->timestamp_interval;
		*ii_ptr++ = c->trade_id_interval;
		*o_ptr++ = c->open;
		*h_ptr++ = c->high;
		*l_ptr++ = c->low;
		*c_ptr++ = c->close;
		*v_ptr++ = c->volume;
	}
	atomsnap_release_version(page_version);

	/* Remember converting context for this chunk */
	end_idx += 1;
	chunk->converting_page_idx = candle_chunk_calc_page_idx(end_idx);
	chunk->converting_row_idx = candle_chunk_calc_row_idx(end_idx);
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
	uint64_t mutable_seq = atomic_load(&list->mutable_seq);
	uint64_t last_seq_converted = atomic_load(&list->last_seq_converted);
	struct atomsnap_version *head_snap = NULL;
	int n, num_completed, num_converted, start_idx, end_idx;

	/* No more candles to convert */
	if (mutable_seq == UINT64_MAX || mutable_seq - 1 == last_seq_converted) {
		return;
	}

	/* Pin the head */
	head_snap = atomsnap_acquire_version(list->head_gate);

	while (chunk != NULL) {
		num_completed = atomic_load(&chunk->num_completed);
		num_converted = atomic_load(&chunk->num_converted);

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
				= trcache_batch_alloc_on_heap(list->batch_candle_count);
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

		n = end_idx - start_idx + 1;
		last_seq_converted += n;
		num_converted += n;
		atomic_store(&chunk->num_converted, num_converted);

		/* The chunk is not fully converted, try it again in later */
		if (num_converted != list->batch_candle_count) {
			list->converting_chunk = chunk;
			break;
		}

		/* If this chunk is fully converted, notice it to the flush worker */
		atomic_fetch_add(&list->unflushed_batch_count, 1);
		chunk = chunk->next;
	}

	/* Unpin head */
	atomsnap_release_version(head_snap);

	atomic_store(&list->last_seq_converted, last_seq_converted);
}

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list: Pointer to the candle chunk list.
 *
 * May invoke user-supplied flush callbacks.
 *
 * The admin thread must ensure that the flush function for a single chunk
 * list is executed by only one worker thread at a time.
 */
void candle_chunk_list_flush(struct candle_chunk_list *list)
{
	int flush_batch_count = list->flush_threshold_batches
		- atomic_load(&list->unflushed_batch_count);
	struct atomsnap_version *head_snap = NULL;
	struct candle_chunk_list_head_version *head_version = NULL;

	if (flush_batch_count < 0) {
		return;
	}

	head_snap = atomsnap_acquire_version(list->head_gate);
	head_version = (struct candle_chunk_list_head_version *)head_snap->object;

	/*
	 * Modifying the head of the list is restricted to a single thread,
	 * coordinated by the admin thread. Thus, only this execution path is
	 * allowed to update the list head, making this head version the latest one.
	 */

	for (int i = 0; i < flush_batch_count; i++) {

	}
}
