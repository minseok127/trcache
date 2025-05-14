/**
 * @file   candle_chunk.c
 * @brief  Implementation of row -> column staging chunk for trcache.
 *
 * This module manages one candle chunk, which buffers real‑time trades into
 * row‑form pages, converts them to column batches, and finally flushes the
 * result. All shared structures are protected by atomsnap gates.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>

#include "core/candle_chunk.h"

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
 * @param   symbol_id:          Integer symbol ID resolved via symbol table.
 * @param   candle_type:        Enum trcache_candle_type.
 * @param   batch_candle_count: Number of candles per column batch (chunk).
 * @param   row_page_count:     Number of row pages per chunk.
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
static struct candle_chunk *craete_candle_chunk(int symbol_id,
	trcache_candle_type candle_type, uint32_t batch_candle_count,
	uint32_t row_page_count)
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

	chunk->column_batch = trcache_batch_alloc_on_heap(batch_candle_count);
	if (chunk->column_batch == NULL) {
		fprintf(stderr, "create_candle_chunk: batch alloc failed\n");
		row_page_version_free(row_page_version);
		atomsnap_destroy_gate(chunk->row_gate);
		pthread_spin_destroy(&chunk->spinlock);
		free(chunk);
		return NULL;
	}

	chunk->column_batch->symbol_id = symbol_id;
	chunk->column_batch->candle_type = candle_type;

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
	void *arg __attribute__((unused)))
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

	free(head_version); /* #candle_chunk_list_head_version */
	free(head_version->snap_version); /* #atomsnap_version */

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
		.aotmsnap_free_impl = candle_chunk_list_head_free,
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

	chunk = create_candle_chunk(list->batch_candle_count, list->row_page_count);
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

	list->last_row_completed = -1;
	list->last_row_converted = -1;
	list->unflushed_batch_count = 0;
	list->tail = chunk;
	list->candle_mutable_chunk = chunk;
	list->converting_chunk = chunk;
	list->update_ops = ctx->update_ops;
	list->flush_ops = ctx->flush_ops;
	list->flush_threshold_batches = ctx->flush_threadhold_batches;
	list->candle_type = ctx->candle_type;
	list->symbol_id = ctx->symbol_id;

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
 * @brief
 *
 * @param
 * @param
 * @param
 * @param
 *
 * @return
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

	/*
	 * This path occurs only once, right after the chunk list is created.
	 * During page initialization, the first candle is also initialized, 
	 * which requires trade data. However, since trade data is not available 
	 * at the time the chunk list is created, this separate path was introduced.
	 */
	if (chunk->mutable_page_idx == -1) {
		if (candle_page_init(chunk, 0, ops, trade) == -1) {
			fprintf(stderr, "candle_chunk_list_apply_trade: init page failed\n");
			return -1;
		}

		chunk->mutable_page_idx = 0;
		chunk->mutable_row_idx = 0;

		/* XXX candle map insert => user can see this candle */

		return 0;
	}

	row_page_version = atomsnap_acquire_version_slot(chunk->row_gate,
		chunk->mutable_page_idx);
	row_page = (struct candle_row_page *)row_page_version->object;

	/* Update candle if it is not closed */
	candle = &(row_page->rows[chunk->mutable_row_idx]);
	if (!ops->is_closed(candle, trade)) {
		pthread_spin_lock(&chunk->spinlock);
		ops->update(candle, trade);
		pthread_spin_unlock(&chunk->spinlock);
		atomsnap_release_version(row_page_version);
		return 0;
	}

	/* Move to the next candle */
	if (chunk->num_completed + 1 == list->batch_candle_count) {
		/* If we should move to the new chunk */
		atomsnap_release_version(row_page_version);

		new_chunk = create_candle_chunk(list->batch_candle_count,
			list->row_page_count);

		if (new_chunk == NULL) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: invalid new chunk\n");
			return -1;
		}

		if (candle_page_init(new_chunk, 0, ops, trade) == -1) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: new chunk init failed\n");
			candle_chunk_destroy(new_chunk);
			return -1;
		}

		new_chunk->mutable_page_idx = 0;
		new_chunk->mutable_row_idx = 0;

		/* Add the new chunk into tail of the linked list */
		chunk->next = new_chunk;
		list->tail = new_chunk;
		list->candle_mutable_chunk = new_chunk;
	} else if (chunk->mutable_row_idx == TRCACHE_ROWS_PER_PAGE - 1) {
		/* If we should move to the next page */
		atomsnap_release_version(row_page_version);

		chunk->mutable_page_idx += 1;

		if (candle_page_init(
				chunk, chunk->mutable_page_idx + 1, ops, trade) == -1) {
			fprintf(stderr,
				"candle_chunk_list_apply_trade: new page init failed\n");
			chunk->mutable_page_idx -= 1; /* rollback */
			return -1;
		}

		chunk->mutable_row_idx = 0;
	} else {
		/* If we can use the same page */
		chunk->mutable_row_idx += 1;
		candle = &(row_page->rows[chunk->mutable_row_idx]);

		pthread_spin_lock(&chunk->spinlock);
		ops->update(candle, trade);
		pthread_spin_unlock(&chunk->spinlock);
		atomsnap_release_version(row_page_version);
	}

	/* XXX candle map insert => user can see this candle */

	/*
	 * Ensure that the previous mutable candle is completed, and a new mutable
	 * candle can be observed outside the module.
	 */
	atomic_store(&chunk->num_completed, chunk->num_completed + 1);
	atomic_store(&list->last_row_completed, list->last_row_completed + 1);

	return 0;
}

/**
 * @brief    Convert all mutable row candles into a column batch.
 *
 * @param    list: Pointer to the candle chunk list.
 *
 * Transforms finalized row-based candles into columnar format.
 */
void candle_chunk_list_convert_to_column_batch(struct candle_chunk_list *list)
{

}

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list: Pointer to the candle chunk list.
 *
 * May invoke user-supplied flush callbacks.
 */
void candle_chunk_list_flush(struct candle_chunk_list *list)
{

}
