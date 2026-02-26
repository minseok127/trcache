/**
 * @file   trade_data_buffer.c
 * @brief  Chunk‐based buffer for trcache_trade_data
 *
 * Producer pushes trade data into chunks; when full, obtains a free
 * chunk from free list or mallocs a new one. Consumers use a cursor to peek
 * and consume entries; when threshold reached, chunk is enqueued back
 * to free list by producer's context.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "meta/trcache_internal.h"
#include "pipeline/trade_data_buffer.h"
#include "utils/list_head.h"
#include "utils/log.h"

#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))

/**
 * @brief   Helper to calculate the cacheline-aligned struct allocation size.
 */
static size_t calculate_chunk_struct_size(void)
{
	return ALIGN_UP(sizeof(struct trade_data_chunk), CACHE_LINE_SIZE);
}

/**
 * @brief   Helper to calculate the 4 KiB-aligned data buffer size.
 *
 * The result is rounded up to TRADE_DATA_BUF_ALIGN so that
 * aligned_alloc(TRADE_DATA_BUF_ALIGN, ...) satisfies its requirement that
 * size is a multiple of the requested alignment.
 */
static size_t calculate_data_buf_size(struct trcache *tc)
{
	size_t raw = (size_t)tc->trades_per_chunk * tc->trade_data_size;
	return ALIGN_UP(raw, TRADE_DATA_BUF_ALIGN);
}

/**
 * @brief   Create and initialize a trade_data_buffer.
 *
 * @param   tc: Pointer to the #trcache.
 * @param   symbol_id: Integer symbol ID.
 *
 * @return  Pointer to buffer, or NULL on failure.
 */
struct trade_data_buffer *trade_data_buffer_init(struct trcache *tc,
	int symbol_id)
{
	struct trade_data_buffer *buf = NULL;
	struct trade_data_chunk *chunk = NULL;
	struct trade_data_buffer_cursor *c = NULL;
	size_t chunk_struct_size, data_buf_size, chunk_total_size;

	if (tc == NULL) {
		errmsg(stderr, "Invalid argument\n");
		return NULL;
	}

	buf = aligned_alloc(CACHE_LINE_SIZE,
		ALIGN_UP(sizeof(struct trade_data_buffer), CACHE_LINE_SIZE));

	if (buf == NULL) {
		errmsg(stderr, "#trade_data_buffer allocation failed\n");
		return NULL;
	}

	memset(buf, 0, sizeof(struct trade_data_buffer));

	buf->trc = tc;
	atomic_init(&buf->memory_usage.value, 0);
	mem_add_atomic(&buf->memory_usage.value, sizeof(struct trade_data_buffer));

	/*
	 * Allocate the chunk struct and its data buffer separately.
	 * chunk_total_size is tracked for memory accounting; it equals
	 * the struct size plus the 4 KiB-aligned data buffer size.
	 */
	chunk_struct_size = calculate_chunk_struct_size();
	data_buf_size = calculate_data_buf_size(tc);
	chunk_total_size = chunk_struct_size + data_buf_size;

	chunk = aligned_alloc(CACHE_LINE_SIZE, chunk_struct_size);
	if (chunk == NULL) {
		errmsg(stderr, "#trade_data_chunk allocation failed\n");
		free(buf);
		return NULL;
	}

	memset(chunk, 0, sizeof(struct trade_data_chunk));

	chunk->data = aligned_alloc(TRADE_DATA_BUF_ALIGN, data_buf_size);
	if (chunk->data == NULL) {
		errmsg(stderr, "trade_data_chunk data buffer allocation failed\n");
		free(chunk);
		free(buf);
		return NULL;
	}

	mem_add_atomic(&buf->memory_usage.value, chunk_total_size);

	INIT_LIST_HEAD(&buf->chunk_list);

	/* Add the first chunk into chunk list */
	INIT_LIST_HEAD(&chunk->list_node);
	atomic_store(&chunk->write_idx, 0);
	atomic_store(&chunk->num_consumed_cursor, 0);
	atomic_store(&chunk->flush_state, TRADE_CHUNK_FLUSH_NOT_READY);
	list_add_tail(&chunk->list_node, &buf->chunk_list);

	/* Init cursors */
	for (int i = 0; i < tc->num_candle_configs; i++) {
		c = &buf->cursor_arr[i];
		c->consume_chunk = chunk;
		c->consume_count = 0;
		c->peek_chunk = chunk;
		c->peek_idx = 0;
		atomic_store_explicit(&c->in_use, 0, memory_order_release);
	}

	buf->produced_count = 0;
	buf->next_tail_write_idx = 0;
	buf->num_cursor = tc->num_candle_configs;
	buf->trc = tc;
	buf->symbol_id = symbol_id;
	buf->chunk_allocation_size = chunk_total_size;
	atomic_store(&buf->num_full_unflushed_chunks, 0);
	atomic_store(&buf->ema_cycles_per_trade_flush, 0);

	return buf;
}

/**
 * @brief   Destroy buffer and free all resources.
 *
 * @param   buf: Buffer to destroy.
 */
void trade_data_buffer_destroy(struct trade_data_buffer *buf)
{
	struct trade_data_chunk *chunk = NULL;
	struct list_head *c = NULL, *n = NULL;

	if (buf == NULL) {
		return;
	}

	/* Free all chunks in list; each chunk owns a separate data buffer. */
	if (!list_empty(&buf->chunk_list)) {
		c = list_get_first(&buf->chunk_list);
		while (c != &buf->chunk_list) {
			n = c->next;
			chunk = __get_trd_chunk_ptr(c);
			free(chunk->data);
			free(chunk);
			mem_sub_atomic(&buf->memory_usage.value,
				buf->chunk_allocation_size);
			c = n;
		}
	}

	mem_sub_atomic(&buf->memory_usage.value, sizeof(struct trade_data_buffer));
	free(buf);
}

/**
 * @brief   Push one trade record into buffer.
 *
 * @param   buf:       Buffer to push into.
 * @param   data:      Trade record to copy.
 * @param   free_list: Linked list pointer holding recycled chunks.
 * @param   thread_id: The feed thread's unique ID.
 *
 * @return  0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const void *data, struct list_head *free_list,
	int thread_id)
{
	struct trade_data_chunk *tail = NULL, *new_chunk = NULL;
	_Atomic(size_t) *free_list_mem_counter =
		&buf->trc->mem_acc.feed_thread_free_list_mem[thread_id].value;
	void *dst_ptr;

	if (!buf || !data) {
		return -1;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

	/*
	 * The user thread exclusively manages chunk allocation and maintains the
	 * free list. Whether a chunk has been fully consumed is determined by a
	 * counter incremented by the consumer threads. It's possible for the
	 * consumer threads to be faster and consume all the way to the tail. In
	 * that case, if the consumer's cursor is pointing to a chunk that has
	 * already been fully used, the user thread might place that chunk back into
	 * the free list. Later, when the consumer thread tries to advance its
	 * cursor, it could end up accessing a freed chunk. To prevent this, the
	 * user thread links a new chunk just before writing the last piece of data
	 * to ensure the consumer's cursor never points to a fully consumed chunk.
	 */
	if ((tail->write_idx + 1) == buf->trc->trades_per_chunk) {
		if (free_list != NULL && !list_empty(free_list)) {
			list_move_tail(free_list->next, &buf->chunk_list);
			new_chunk = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

			/* Transfer memory ownership from free list to this buffer */
			mem_sub_atomic(free_list_mem_counter,
				buf->chunk_allocation_size);
			mem_add_atomic(&buf->memory_usage.value,
				buf->chunk_allocation_size);
		} else {
			/*
			 * The local free list is empty, so we must allocate a new
			 * chunk. Before allocating, check if the global memory
			 * usage has exceeded the hard limit.
			 *
			 * This check acts as the final back-pressure mechanism,
			 * distinct from the 'memory_pressure' flag (which only
			 * controls the object pool recycling policy, not
			 * new allocations).
			 */
			const size_t limit = buf->trc->total_memory_limit;
			size_t current_usage = atomic_load_explicit(
				&buf->trc->mem_acc.total_usage.value,
				memory_order_acquire);

			if (current_usage >= limit) {
				/*
				 * Memory limit reached. Fail the push immediately.
				 * The feed thread is expected to retry this feed.
				 */
				return -1;
			}

			new_chunk = aligned_alloc(CACHE_LINE_SIZE,
				calculate_chunk_struct_size());

			if (new_chunk == NULL) {
				errmsg(stderr, "#trade_data_chunk allocation failed\n");
				return -1;
			}

			/*
			 * Zero only the struct. The data buffer is allocated
			 * separately below to preserve 4 KiB alignment for DMA.
			 */
			memset(new_chunk, 0, sizeof(struct trade_data_chunk));

			new_chunk->data = aligned_alloc(TRADE_DATA_BUF_ALIGN,
				buf->trc->trade_data_buf_size);

			if (new_chunk->data == NULL) {
				errmsg(stderr,
					"trade_data_chunk data buffer allocation failed\n");
				free(new_chunk);
				return -1;
			}

			/* Add new memory to this buffer's tracking */
			mem_add_atomic(&buf->memory_usage.value,
				buf->chunk_allocation_size);

			INIT_LIST_HEAD(&new_chunk->list_node);
			list_add_tail(&new_chunk->list_node, &buf->chunk_list);
		}

		buf->next_tail_write_idx = 0;

		atomic_store_explicit(&new_chunk->write_idx, 0,
			memory_order_release);
		atomic_store_explicit(&new_chunk->num_consumed_cursor, 0,
			memory_order_release);
		atomic_store_explicit(&new_chunk->flush_state,
			TRADE_CHUNK_FLUSH_NOT_READY, memory_order_relaxed);
		/*
		 * new_chunk->data is intentionally not reset here:
		 * it points to the chunk's permanent 4 KiB-aligned buffer
		 * (either reused from the free list or freshly allocated
		 * above).
		 */
	} else {
		buf->next_tail_write_idx += 1;
	}

	/* Copy user data into the 4 KiB-aligned data buffer. */
	dst_ptr = (uint8_t *)tail->data +
		(tail->write_idx * buf->trc->trade_data_size);
	memcpy(dst_ptr, data, buf->trc->trade_data_size);

	/*
	 * Consumers must access the last entry only after
	 * the new chunk has been linked.
	 */
	atomic_store_explicit(&tail->write_idx, tail->write_idx + 1,
		memory_order_release);

	/*
	 * If this write filled the chunk, mark it as needing a flush.
	 * The new tail was already linked above, so 'tail' is now the
	 * second-to-last chunk and will no longer be written to.
	 */
	if (tail->write_idx == buf->trc->trades_per_chunk &&
			buf->trc->trade_flush_ops.flush != NULL) {
		atomic_store_explicit(&tail->flush_state,
			TRADE_CHUNK_FLUSH_NEEDED, memory_order_release);
		atomic_fetch_add_explicit(&buf->num_full_unflushed_chunks,
			1, memory_order_release);
	}

	buf->produced_count += 1;

	return 0;
}

/**
 * @brief   Peek at next entries for a cursor.
 *
 * @param   buf:              Buffer to peek from.
 * @param   cursor:           Caller-owned cursor.
 * @param   data_array (out): Pointer to first entry to read.
 * @param   count (out):      Number of contiguous entries available.
 *
 * @return  1 if some entries are available, 0 if empty or error.
 */
int trade_data_buffer_peek(struct trade_data_buffer *buf,
	struct trade_data_buffer_cursor *cursor,
	void **data_array,
	int *count)
{
	struct trade_data_chunk *chunk = NULL;
	int write_idx;

	if (!buf || !cursor || !data_array || !count) {
		if (count) {
			*count = 0;
		}
		return 0;
	}

	assert(cursor->peek_chunk != NULL);
	chunk = cursor->peek_chunk;
	write_idx = atomic_load_explicit(
		&chunk->write_idx, memory_order_acquire);

	if (cursor->peek_idx == write_idx) {
		*count = 0;
		return 0;
	} 

	*data_array = (uint8_t *)chunk->data +
		(cursor->peek_idx * buf->trc->trade_data_size);
	*count = write_idx - cursor->peek_idx;

	/* advance peek cursor */
	cursor->peek_idx += *count;

	if (cursor->peek_idx == buf->trc->trades_per_chunk) {
		assert(&chunk->list_node != list_get_last(&buf->chunk_list));
		chunk = __get_trd_chunk_ptr(chunk->list_node.next);
		cursor->peek_chunk = chunk;
		cursor->peek_idx = 0;
	}

	return 1;
}

/**
 * @brief   Consume all entries that the caller has already peeked.
 *
 * @param   buf:	 Buffer to consume from.
 * @param   cursor:	 Caller-managed cursor (same one used for peek).
 * @param   count:   Number of data items fetched via peek().
 */
void trade_data_buffer_consume(struct trade_data_buffer	*buf,
	struct trade_data_buffer_cursor *cursor, int count)
{
	struct trade_data_chunk *chunk = NULL;
	struct list_head *c = NULL;

	if (!buf || !cursor) {
		return;
	}

	cursor->consume_count += count;

	assert(cursor->peek_chunk != NULL && cursor->consume_chunk != NULL);

	if (cursor->consume_chunk == cursor->peek_chunk) {
		return;
	}

	chunk = cursor->consume_chunk;

	while (chunk != cursor->peek_chunk) {
		c = chunk->list_node.next;

		assert(c != &buf->chunk_list);

		/*
		 * Dereferencing the chunk must occur before incrementing the counter
		 * because this counter is the criterion for deciding when to free the
		 * chunk. Incrementing the counter before dereferencing could result in
		 * accessing a freed chunk.
		 */
		atomic_fetch_add_explicit(&chunk->num_consumed_cursor, 1,
			memory_order_release);

		chunk = __get_trd_chunk_ptr(c);
	}

	cursor->consume_chunk = chunk;
}

/**
 * @brief   Move free chunks into the free list.
 *
 * This function attempts to reclaim memory in several stages. First, it tries
 * to move fully consumed chunks to the 'free_list'. If the memory limit is
 * exceeded, it frees the chunks directly instead.
 *
 * @param   buf:                 Buffer to reap the free chunks.
 * @param   free_list:           Linked list pointer holding recycled chunks.
 * @param   thread_id:           The feed thread's unique ID.
 */
void trade_data_buffer_reap_free_chunks(struct trade_data_buffer *buf,
	struct list_head *free_list, int thread_id, int flush_enabled)
{
	struct trade_data_chunk *tail = NULL, *chunk = NULL, *c = NULL;
	struct list_head *first = NULL, *last = NULL, *node = NULL, *next = NULL;
	bool memory_pressure = atomic_load_explicit(
		&buf->trc->mem_acc.memory_pressure, memory_order_acquire);
	_Atomic(size_t) *free_list_mem_counter =
		&buf->trc->mem_acc.feed_thread_free_list_mem[thread_id].value;
	size_t total_reaped_size = 0;

	if (free_list == NULL || list_empty(&buf->chunk_list)) {
		return;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

	first = list_get_first(&buf->chunk_list);

	chunk = __get_trd_chunk_ptr(first);

	/*
	 * Note that we should remain at least one node in the list.
	 * See the comment in the trade_data_buffer_push().
	 *
	 * A chunk is reclaimable when all candle-type cursors have moved
	 * past it AND, if trade flush is configured, its flush is done.
	 */
	while (chunk != tail) {
		if (atomic_load_explicit(&chunk->num_consumed_cursor,
				memory_order_acquire) != buf->num_cursor) {
			break;
		}

		if (flush_enabled &&
				atomic_load_explicit(&chunk->flush_state,
					memory_order_acquire)
				!= TRADE_CHUNK_FLUSH_DONE) {
			break;
		}

		last = (struct list_head *) &chunk->list_node;
		total_reaped_size += buf->chunk_allocation_size;
		chunk = __get_trd_chunk_ptr(chunk->list_node.next);
	}

	if (last != NULL) {
		/*
		 * Always subtract the memory from the buffer's active count.
		 */
		mem_sub_atomic(&buf->memory_usage.value, total_reaped_size);

		if (memory_pressure) {
			node = first;
			while (node != NULL) {
				c = __get_trd_chunk_ptr(node);
				next = (node == last) ? NULL : node->next;
				free(c->data);
				free(c);
				node = next;
			}

			buf->chunk_list.next = &chunk->list_node;
			chunk->list_node.prev = &buf->chunk_list;
		} else {
			/* Transfer memory ownership to the free list */
			mem_add_atomic(free_list_mem_counter, total_reaped_size);
			list_bulk_move_tail(free_list, first, last);
		}
	}
}

/**
 * @brief   Initiate or poll the flush of a single chunk.
 *
 * If the chunk is NEEDED, calls ops->flush() and transitions it to
 * IN_FLIGHT. Then polls ops->is_done(); if done, transitions to DONE
 * and decrements num_full_unflushed_chunks.
 *
 * Reads the actual trade count from @chunk->write_idx so it works
 * correctly for both full non-tail chunks (write_idx == trades_per_chunk)
 * and partial tail chunks (write_idx < trades_per_chunk).
 *
 * @param   buf:   Owning buffer (for context and counters).
 * @param   chunk: Target chunk; flush_state must be NEEDED or IN_FLIGHT.
 * @param   ops:   Flush callback operations.
 *
 * @return  1 if the chunk transitioned to DONE, 0 if still in progress.
 */
static int flush_complete_chunk(struct trade_data_buffer *buf,
	struct trade_data_chunk *chunk,
	const struct trcache_trade_flush_ops *ops)
{
	if (atomic_load_explicit(&chunk->flush_state, memory_order_acquire)
			== TRADE_CHUNK_FLUSH_NEEDED) {
		/*
		 * flush() has not been called yet. Read write_idx for the
		 * actual count; for completed non-tail chunks this equals
		 * trades_per_chunk, for a partial tail it is the real count.
		 */
		int num_trades = atomic_load_explicit(
			&chunk->write_idx, memory_order_acquire);

		ops->flush(buf->trc, buf->symbol_id,
			chunk->data, num_trades, ops->ctx);

		atomic_store_explicit(&chunk->flush_state,
			TRADE_CHUNK_FLUSH_IN_FLIGHT, memory_order_release);
	}

	/* flush() was already called; poll for completion. */
	if (!ops->is_done(buf->trc, chunk->data, ops->ctx)) {
		return 0;
	}

	atomic_store_explicit(&chunk->flush_state,
		TRADE_CHUNK_FLUSH_DONE, memory_order_release);
	atomic_fetch_sub_explicit(&buf->num_full_unflushed_chunks,
		1, memory_order_release);
	return 1;
}

/**
 * @brief   Flush full trade chunks using the provided flush callbacks.
 *
 * Iterates all chunks ahead of the current tail. For each chunk in the
 * NEEDED or IN_FLIGHT state, drives the flush via flush_complete_chunk().
 * The active tail chunk is never touched here; use
 * trade_data_buffer_finalize() to flush it during shutdown.
 *
 * @param   buf:  Buffer whose chunks to flush.
 * @param   ops:  Trade flush callback operations.
 *
 * @return  Number of chunks whose flush completed in this call.
 */
int trade_data_buffer_flush_full_chunks(struct trade_data_buffer *buf,
	const struct trcache_trade_flush_ops *ops)
{
	struct trade_data_chunk *tail, *chunk;
	struct list_head *node;
	int completed = 0;

	if (buf == NULL || ops == NULL || ops->flush == NULL) {
		return 0;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

	node = list_get_first(&buf->chunk_list);
	while (node != &buf->chunk_list) {
		chunk = __get_trd_chunk_ptr(node);
		node = node->next;

		/* Never touch the active tail chunk. */
		if (chunk == tail) {
			break;
		}

		int state = atomic_load_explicit(&chunk->flush_state,
			memory_order_acquire);
		if (state == TRADE_CHUNK_FLUSH_NEEDED ||
				state == TRADE_CHUNK_FLUSH_IN_FLIGHT) {
			completed += flush_complete_chunk(buf, chunk, ops);
		}
	}

	return completed;
}

/**
 * @brief   Flush all remaining trade chunks, including any partial tail.
 *
 * Intended for use during trcache_destroy(). Marks the current tail chunk
 * as needing a flush if it contains any data, then busy-polls until all
 * chunks (full and partial) have completed their flush.
 *
 * The tail chunk is handled directly here because
 * trade_data_buffer_flush_full_chunks() deliberately skips it during
 * normal operation (the producer may still be writing to it).
 *
 * @param   buf:  Buffer to finalize.
 * @param   ops:  Trade flush callback operations.
 */
void trade_data_buffer_finalize(struct trade_data_buffer *buf,
	const struct trcache_trade_flush_ops *ops)
{
	struct trade_data_chunk *tail;
	int write_idx;

	if (buf == NULL || ops == NULL || ops->flush == NULL) {
		return;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));
	write_idx = atomic_load_explicit(&tail->write_idx,
		memory_order_acquire);

	/*
	 * If the tail contains data that has not yet been marked for flush,
	 * mark it now. write_idx is intentionally NOT overridden so that
	 * flush_complete_chunk() passes the real trade count to the callback.
	 * The producer has already stopped (destroy path), so write_idx is
	 * stable and this write is race-free.
	 */
	if (write_idx > 0 &&
			atomic_load_explicit(&tail->flush_state,
				memory_order_acquire)
			== TRADE_CHUNK_FLUSH_NOT_READY) {
		atomic_store_explicit(&tail->flush_state,
			TRADE_CHUNK_FLUSH_NEEDED, memory_order_release);
		atomic_fetch_add_explicit(&buf->num_full_unflushed_chunks,
			1, memory_order_release);
	}

	/*
	 * Busy-poll until every chunk is flushed. Non-tail chunks are
	 * handled by flush_full_chunks(); the tail is handled directly here
	 * because flush_full_chunks() always skips it.
	 *
	 * In the destroy path there is no concurrent producer, so this loop
	 * always terminates.
	 */
	while (atomic_load_explicit(&buf->num_full_unflushed_chunks,
			memory_order_acquire) > 0) {
		trade_data_buffer_flush_full_chunks(buf, ops);

		int tail_state = atomic_load_explicit(&tail->flush_state,
			memory_order_acquire);
		if (tail_state == TRADE_CHUNK_FLUSH_NEEDED ||
				tail_state == TRADE_CHUNK_FLUSH_IN_FLIGHT) {
			flush_complete_chunk(buf, tail, ops);
		}
	}
}
