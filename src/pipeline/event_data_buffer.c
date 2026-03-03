/**
 * @file   event_data_buffer.c
 * @brief  Block-based buffer for event data (trade, book, etc.)
 *
 * Producer pushes event data into blocks; when full, obtains a free
 * block from free list or mallocs a new one. Consumers use a cursor to peek
 * and consume entries; when threshold reached, block is enqueued back
 * to free list by producer's context.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "meta/trcache_internal.h"
#include "pipeline/event_data_buffer.h"
#include "utils/list_head.h"
#include "utils/log.h"

#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))

/**
 * @brief   Helper to calculate the cacheline-aligned struct allocation size.
 */
static size_t calculate_block_struct_size(void)
{
	return ALIGN_UP(sizeof(struct event_data_block), CACHE_LINE_SIZE);
}

/**
 * @brief   Create and initialize an event_data_buffer.
 *
 * @param   tc:               Pointer to the #trcache.
 * @param   symbol_id:        Integer symbol ID.
 * @param   event_size:       Size of one event record in bytes.
 * @param   events_per_block: Number of events per block.
 * @param   event_buf_size:   4 KiB-aligned data buffer size per block.
 * @param   num_cursors:      Number of cursors to initialize.
 *
 * @return  Pointer to buffer, or NULL on failure.
 */
struct event_data_buffer *event_data_buffer_init(struct trcache *tc,
	int symbol_id, size_t event_size, int events_per_block,
	size_t event_buf_size, int num_cursors)
{
	struct event_data_buffer *buf = NULL;
	struct event_data_block *block = NULL;
	struct event_data_buffer_cursor *c = NULL;
	size_t block_struct_size, block_total_size;

	if (tc == NULL) {
		errmsg(stderr, "Invalid argument\n");
		return NULL;
	}

	buf = aligned_alloc(CACHE_LINE_SIZE,
		ALIGN_UP(sizeof(struct event_data_buffer), CACHE_LINE_SIZE));

	if (buf == NULL) {
		errmsg(stderr, "#event_data_buffer allocation failed\n");
		return NULL;
	}

	memset(buf, 0, sizeof(struct event_data_buffer));

	buf->trc = tc;
	buf->event_size = event_size;
	buf->events_per_block = events_per_block;
	buf->event_buf_size = event_buf_size;
	atomic_init(&buf->memory_usage.value, 0);
	mem_add_atomic(&buf->memory_usage.value, sizeof(struct event_data_buffer));

	/*
	 * Allocate the block struct and its data buffer separately.
	 * block_total_size is tracked for memory accounting; it equals
	 * the struct size plus the 4 KiB-aligned data buffer size.
	 */
	block_struct_size = calculate_block_struct_size();
	block_total_size = block_struct_size + event_buf_size;

	block = aligned_alloc(CACHE_LINE_SIZE, block_struct_size);
	if (block == NULL) {
		errmsg(stderr, "#event_data_block allocation failed\n");
		free(buf);
		return NULL;
	}

	memset(block, 0, sizeof(struct event_data_block));

	block->data = aligned_alloc(EVENT_DATA_BUF_ALIGN, event_buf_size);
	if (block->data == NULL) {
		errmsg(stderr, "event_data_block data buffer allocation failed\n");
		free(block);
		free(buf);
		return NULL;
	}

	mem_add_atomic(&buf->memory_usage.value, block_total_size);

	INIT_LIST_HEAD(&buf->block_list);

	/* Add the first block into block list */
	INIT_LIST_HEAD(&block->list_node);
	atomic_store(&block->write_idx, 0);
	atomic_store(&block->num_consumed_cursor, 0);
	atomic_store(&block->flush_state, EVENT_BLOCK_FLUSH_NOT_READY);
	list_add_tail(&block->list_node, &buf->block_list);

	/* Init cursors */
	for (int i = 0; i < num_cursors; i++) {
		c = &buf->cursor_arr[i];
		c->consume_block = block;
		c->consume_count = 0;
		c->peek_block = block;
		c->peek_idx = 0;
		atomic_store_explicit(&c->in_use, 0, memory_order_release);
	}

	buf->produced_count = 0;
	buf->next_tail_write_idx = 0;
	buf->num_cursor = num_cursors;
	buf->trc = tc;
	buf->symbol_id = symbol_id;
	buf->block_allocation_size = block_total_size;
	atomic_store(&buf->num_full_unflushed_blocks, 0);
	atomic_store(&buf->ema_cycles_per_flush, 0);

	return buf;
}

/**
 * @brief   Destroy buffer and free all resources.
 *
 * @param   buf: Buffer to destroy.
 */
void event_data_buffer_destroy(struct event_data_buffer *buf)
{
	struct event_data_block *block = NULL;
	struct list_head *c = NULL, *n = NULL;

	if (buf == NULL) {
		return;
	}

	/* Free all blocks in list; each block owns a separate data buffer. */
	if (!list_empty(&buf->block_list)) {
		c = list_get_first(&buf->block_list);
		while (c != &buf->block_list) {
			n = c->next;
			block = __get_evt_block_ptr(c);
			free(block->data);
			free(block);
			mem_sub_atomic(&buf->memory_usage.value,
				buf->block_allocation_size);
			c = n;
		}
	}

	mem_sub_atomic(&buf->memory_usage.value, sizeof(struct event_data_buffer));
	free(buf);
}

/**
 * @brief   Push one event record into buffer.
 *
 * @param   buf:       Buffer to push into.
 * @param   data:      Event record to copy.
 * @param   free_list: Linked list pointer holding recycled blocks.
 * @param   thread_id: The feed thread's unique ID.
 *
 * @return  0 on success, -1 on error.
 */
int event_data_buffer_push(struct event_data_buffer *buf,
	const void *data, struct list_head *free_list,
	int thread_id)
{
	struct event_data_block *tail = NULL, *new_block = NULL;
	_Atomic(size_t) *free_list_mem_counter =
		&buf->trc->mem_acc.feed_thread_free_list_mem[thread_id].value;
	void *dst_ptr;

	if (!buf || !data) {
		return -1;
	}

	tail = __get_evt_block_ptr(list_get_last(&buf->block_list));

	/*
	 * The user thread exclusively manages block allocation and maintains the
	 * free list. Whether a block has been fully consumed is determined by a
	 * counter incremented by the consumer threads. It's possible for the
	 * consumer threads to be faster and consume all the way to the tail. In
	 * that case, if the consumer's cursor is pointing to a block that has
	 * already been fully used, the user thread might place that block back into
	 * the free list. Later, when the consumer thread tries to advance its
	 * cursor, it could end up accessing a freed block. To prevent this, the
	 * user thread links a new block just before writing the last piece of data
	 * to ensure the consumer's cursor never points to a fully consumed block.
	 */
	if ((tail->write_idx + 1) == buf->events_per_block) {
		if (free_list != NULL && !list_empty(free_list)) {
			list_move_tail(free_list->next, &buf->block_list);
			new_block = __get_evt_block_ptr(list_get_last(&buf->block_list));

			/* Transfer memory ownership from free list to this buffer */
			mem_sub_atomic(free_list_mem_counter,
				buf->block_allocation_size);
			mem_add_atomic(&buf->memory_usage.value,
				buf->block_allocation_size);
		} else {
			/*
			 * The local free list is empty, so we must allocate a new
			 * block. Before allocating, check if the global memory
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

			new_block = aligned_alloc(CACHE_LINE_SIZE,
				calculate_block_struct_size());

			if (new_block == NULL) {
				errmsg(stderr, "#event_data_block allocation failed\n");
				return -1;
			}

			/*
			 * Zero only the struct. The data buffer is allocated
			 * separately below to preserve 4 KiB alignment for DMA.
			 */
			memset(new_block, 0, sizeof(struct event_data_block));

			new_block->data = aligned_alloc(EVENT_DATA_BUF_ALIGN,
				buf->event_buf_size);

			if (new_block->data == NULL) {
				errmsg(stderr,
					"event_data_block data buffer allocation failed\n");
				free(new_block);
				return -1;
			}

			/* Add new memory to this buffer's tracking */
			mem_add_atomic(&buf->memory_usage.value,
				buf->block_allocation_size);

			INIT_LIST_HEAD(&new_block->list_node);
			list_add_tail(&new_block->list_node, &buf->block_list);
		}

		buf->next_tail_write_idx = 0;

		atomic_store_explicit(&new_block->write_idx, 0,
			memory_order_release);
		atomic_store_explicit(&new_block->num_consumed_cursor, 0,
			memory_order_release);
		atomic_store_explicit(&new_block->flush_state,
			EVENT_BLOCK_FLUSH_NOT_READY, memory_order_relaxed);
		/*
		 * new_block->data is intentionally not reset here:
		 * it points to the block's permanent 4 KiB-aligned buffer
		 * (either reused from the free list or freshly allocated
		 * above).
		 */
	} else {
		buf->next_tail_write_idx += 1;
	}

	/* Copy user data into the 4 KiB-aligned data buffer. */
	dst_ptr = (uint8_t *)tail->data +
		(tail->write_idx * buf->event_size);
	memcpy(dst_ptr, data, buf->event_size);

	/*
	 * Consumers must access the last entry only after
	 * the new block has been linked.
	 */
	atomic_store_explicit(&tail->write_idx, tail->write_idx + 1,
		memory_order_release);

	/*
	 * If this write filled the block, mark it as needing a flush.
	 * The new tail was already linked above, so 'tail' is now the
	 * second-to-last block and will no longer be written to.
	 */
	if (tail->write_idx == buf->events_per_block &&
			buf->trc->trade_flush_ops.flush != NULL) {
		atomic_store_explicit(&tail->flush_state,
			EVENT_BLOCK_FLUSH_NEEDED, memory_order_release);
		atomic_fetch_add_explicit(&buf->num_full_unflushed_blocks,
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
int event_data_buffer_peek(struct event_data_buffer *buf,
	struct event_data_buffer_cursor *cursor,
	void **data_array,
	int *count)
{
	struct event_data_block *block = NULL;
	int write_idx;

	if (!buf || !cursor || !data_array || !count) {
		if (count) {
			*count = 0;
		}
		return 0;
	}

	assert(cursor->peek_block != NULL);
	block = cursor->peek_block;
	write_idx = atomic_load_explicit(
		&block->write_idx, memory_order_acquire);

	if (cursor->peek_idx == write_idx) {
		*count = 0;
		return 0;
	}

	*data_array = (uint8_t *)block->data +
		(cursor->peek_idx * buf->event_size);
	*count = write_idx - cursor->peek_idx;

	/* advance peek cursor */
	cursor->peek_idx += *count;

	if (cursor->peek_idx == buf->events_per_block) {
		assert(&block->list_node != list_get_last(&buf->block_list));
		block = __get_evt_block_ptr(block->list_node.next);
		cursor->peek_block = block;
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
void event_data_buffer_consume(struct event_data_buffer *buf,
	struct event_data_buffer_cursor *cursor, int count)
{
	struct event_data_block *block = NULL;
	struct list_head *c = NULL;

	if (!buf || !cursor) {
		return;
	}

	cursor->consume_count += count;

	assert(cursor->peek_block != NULL && cursor->consume_block != NULL);

	if (cursor->consume_block == cursor->peek_block) {
		return;
	}

	block = cursor->consume_block;

	while (block != cursor->peek_block) {
		c = block->list_node.next;

		assert(c != &buf->block_list);

		/*
		 * Dereferencing the block must occur before incrementing the counter
		 * because this counter is the criterion for deciding when to free the
		 * block. Incrementing the counter before dereferencing could result in
		 * accessing a freed block.
		 */
		atomic_fetch_add_explicit(&block->num_consumed_cursor, 1,
			memory_order_release);

		block = __get_evt_block_ptr(c);
	}

	cursor->consume_block = block;
}

/**
 * @brief   Move free blocks into the free list.
 *
 * This function attempts to reclaim memory in several stages. First, it tries
 * to move fully consumed blocks to the 'free_list'. If the memory limit is
 * exceeded, it frees the blocks directly instead.
 *
 * @param   buf:                 Buffer to reap the free blocks.
 * @param   free_list:           Linked list pointer holding recycled blocks.
 * @param   thread_id:           The feed thread's unique ID.
 * @param   flush_enabled:       Non-zero if flush ops are configured.
 */
void event_data_buffer_reap_free_blocks(struct event_data_buffer *buf,
	struct list_head *free_list, int thread_id, int flush_enabled)
{
	struct event_data_block *tail = NULL, *block = NULL, *c = NULL;
	struct list_head *first = NULL, *last = NULL, *node = NULL, *next = NULL;
	bool memory_pressure = atomic_load_explicit(
		&buf->trc->mem_acc.memory_pressure, memory_order_acquire);
	_Atomic(size_t) *free_list_mem_counter =
		&buf->trc->mem_acc.feed_thread_free_list_mem[thread_id].value;
	size_t total_reaped_size = 0;

	if (free_list == NULL || list_empty(&buf->block_list)) {
		return;
	}

	tail = __get_evt_block_ptr(list_get_last(&buf->block_list));

	first = list_get_first(&buf->block_list);

	block = __get_evt_block_ptr(first);

	/*
	 * Note that we should remain at least one node in the list.
	 * See the comment in the event_data_buffer_push().
	 *
	 * A block is reclaimable when all cursors have moved past it
	 * AND, if flush is configured, its flush is done.
	 */
	while (block != tail) {
		if (atomic_load_explicit(&block->num_consumed_cursor,
				memory_order_acquire) != buf->num_cursor) {
			break;
		}

		if (flush_enabled &&
				atomic_load_explicit(&block->flush_state,
					memory_order_acquire)
				!= EVENT_BLOCK_FLUSH_DONE) {
			break;
		}

		last = (struct list_head *) &block->list_node;
		total_reaped_size += buf->block_allocation_size;
		block = __get_evt_block_ptr(block->list_node.next);
	}

	if (last != NULL) {
		/*
		 * Always subtract the memory from the buffer's active count.
		 */
		mem_sub_atomic(&buf->memory_usage.value, total_reaped_size);

		if (memory_pressure) {
			node = first;
			while (node != NULL) {
				c = __get_evt_block_ptr(node);
				next = (node == last) ? NULL : node->next;
				free(c->data);
				free(c);
				node = next;
			}

			buf->block_list.next = &block->list_node;
			block->list_node.prev = &buf->block_list;
		} else {
			/* Transfer memory ownership to the free list */
			mem_add_atomic(free_list_mem_counter, total_reaped_size);
			list_bulk_move_tail(free_list, first, last);
		}
	}
}

/**
 * @brief   Initiate or poll the flush of a single block.
 *
 * If the block is NEEDED, calls ops->flush() and transitions it to
 * IN_FLIGHT. Then polls ops->is_done(); if done, transitions to DONE
 * and decrements num_full_unflushed_blocks.
 *
 * Reads the actual event count from @block->write_idx so it works
 * correctly for both full non-tail blocks (write_idx == events_per_block)
 * and partial tail blocks (write_idx < events_per_block).
 *
 * @param   buf:   Owning buffer (for context and counters).
 * @param   block: Target block; flush_state must be NEEDED or IN_FLIGHT.
 * @param   ops:   Flush callback operations.
 *
 * @return  1 if the block transitioned to DONE, 0 if still in progress.
 */
static int event_flush_drive_block(struct event_data_buffer *buf,
	struct event_data_block *block,
	const struct event_data_flush_ops *ops)
{
	if (atomic_load_explicit(&block->flush_state, memory_order_acquire)
			== EVENT_BLOCK_FLUSH_NEEDED) {
		/*
		 * flush() has not been called yet. Read write_idx for the
		 * actual count; for completed non-tail blocks this equals
		 * events_per_block, for a partial tail it is the real count.
		 */
		int num_events = atomic_load_explicit(
			&block->write_idx, memory_order_acquire);

		ops->flush(buf->trc, buf->symbol_id,
			block->data, num_events, ops->ctx);

		atomic_store_explicit(&block->flush_state,
			EVENT_BLOCK_FLUSH_IN_FLIGHT, memory_order_release);
	}

	/* flush() was already called; poll for completion. */
	if (!ops->is_done(buf->trc, block->data, ops->ctx)) {
		return 0;
	}

	atomic_store_explicit(&block->flush_state,
		EVENT_BLOCK_FLUSH_DONE, memory_order_release);
	atomic_fetch_sub_explicit(&buf->num_full_unflushed_blocks,
		1, memory_order_release);
	return 1;
}

/**
 * @brief   Flush full event blocks using the provided flush callbacks.
 *
 * Iterates all blocks ahead of the current tail. For each block in the
 * NEEDED or IN_FLIGHT state, drives the flush via event_flush_drive_block().
 * The active tail block is never touched here; use
 * event_data_buffer_finalize() to flush it during shutdown.
 *
 * @param   buf:  Buffer whose blocks to flush.
 * @param   ops:  Flush callback operations.
 *
 * @return  Number of blocks whose flush completed in this call.
 */
int event_data_buffer_flush_full_blocks(struct event_data_buffer *buf,
	const struct event_data_flush_ops *ops)
{
	struct event_data_block *tail, *block;
	struct list_head *node;
	int completed = 0;

	if (buf == NULL || ops == NULL || ops->flush == NULL) {
		return 0;
	}

	tail = __get_evt_block_ptr(list_get_last(&buf->block_list));

	node = list_get_first(&buf->block_list);
	while (node != &buf->block_list) {
		block = __get_evt_block_ptr(node);
		node = node->next;

		/* Never touch the active tail block. */
		if (block == tail) {
			break;
		}

		/*
		 * Skip DONE blocks explicitly: unlike batch flush, where the
		 * atomsnap head advances past DONE blocks before the next call,
		 * DONE blocks remain in the list until reaped. The state
		 * check here prevents calling event_flush_drive_block() on them.
		 */
		int state = atomic_load_explicit(&block->flush_state,
			memory_order_acquire);
		if (state == EVENT_BLOCK_FLUSH_NEEDED ||
				state == EVENT_BLOCK_FLUSH_IN_FLIGHT) {
			completed += event_flush_drive_block(buf, block, ops);
		}
	}

	return completed;
}

/**
 * @brief   Flush all remaining event blocks, including any partial tail.
 *
 * Intended for use during trcache_destroy(). Marks the current tail block
 * as needing a flush if it contains any data, then busy-polls until all
 * blocks (full and partial) have completed their flush.
 *
 * The tail block is handled directly here because
 * event_data_buffer_flush_full_blocks() deliberately skips it during
 * normal operation (the producer may still be writing to it).
 *
 * @param   buf:  Buffer to finalize.
 * @param   ops:  Flush callback operations.
 */
void event_data_buffer_finalize(struct event_data_buffer *buf,
	const struct event_data_flush_ops *ops)
{
	struct event_data_block *tail;
	int write_idx;

	if (buf == NULL || ops == NULL || ops->flush == NULL) {
		return;
	}

	tail = __get_evt_block_ptr(list_get_last(&buf->block_list));
	write_idx = atomic_load_explicit(&tail->write_idx,
		memory_order_acquire);

	/*
	 * If the tail contains data that has not yet been marked for flush,
	 * mark it now. write_idx is intentionally NOT overridden so that
	 * event_flush_drive_block() passes the real event count to the
	 * callback. The producer has already stopped (destroy path), so
	 * write_idx is stable and this write is race-free.
	 */
	if (write_idx > 0 &&
			atomic_load_explicit(&tail->flush_state, memory_order_acquire)
				== EVENT_BLOCK_FLUSH_NOT_READY) {
		atomic_store_explicit(&tail->flush_state,
			EVENT_BLOCK_FLUSH_NEEDED, memory_order_release);
		atomic_fetch_add_explicit(&buf->num_full_unflushed_blocks,
			1, memory_order_release);
	}

	/*
	 * Busy-poll until every block is flushed. Non-tail blocks are
	 * handled by flush_full_blocks(); the tail is handled directly here
	 * because flush_full_blocks() always skips it.
	 *
	 * In the destroy path there is no concurrent producer, so this loop
	 * always terminates.
	 */
	while (atomic_load_explicit(&buf->num_full_unflushed_blocks,
			memory_order_acquire) > 0) {
		event_data_buffer_flush_full_blocks(buf, ops);

		int tail_state = atomic_load_explicit(&tail->flush_state,
			memory_order_acquire);
		if (tail_state == EVENT_BLOCK_FLUSH_NEEDED ||
				tail_state == EVENT_BLOCK_FLUSH_IN_FLIGHT) {
			event_flush_drive_block(buf, tail, ops);
		}
	}
}
