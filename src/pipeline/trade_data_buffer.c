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

#include "pipeline/trade_data_buffer.h"
#include "utils/list_head.h"
#include "utils/log.h"

/**
 * @brief   Create and initialize a trade_data_buffer.
 *
 * @param   tc: Pointer to the #trcache.
 *
 * @return  Pointer to buffer, or NULL on failure.
 */
struct trade_data_buffer *trade_data_buffer_init(struct trcache *tc)
{
	struct trade_data_buffer *buf = NULL;
	struct trade_data_chunk *chunk = NULL;
	struct trade_data_buffer_cursor *c = NULL;

	if (tc == NULL) {
		errmsg(stderr, "Invalid argument\n");
		return NULL;
	}

	buf = malloc(sizeof(struct trade_data_buffer));

	if (buf == NULL) {
		errmsg(stderr, "#trade_data_buffer allocation failed\n");
		return NULL;
	}

	buf->mem_acc = &tc->mem_acc;

	memstat_add(&buf->mem_acc->ms, MEMSTAT_TRADE_DATA_BUFFER,
		sizeof(struct trade_data_buffer));

	chunk = malloc(sizeof(struct trade_data_chunk));

	if (chunk == NULL) {
		errmsg(stderr, "#trade_data_chunk allocation failed\n");
		free(buf);
		return NULL;
	}

	memstat_add(&buf->mem_acc->ms, MEMSTAT_TRADE_DATA_BUFFER,
		sizeof(struct trade_data_chunk));

	INIT_LIST_HEAD(&buf->chunk_list);

	/* Add the first chunk into chunk list */
	INIT_LIST_HEAD(&chunk->list_node);
	atomic_store(&chunk->write_idx, 0);
	atomic_store(&chunk->num_consumed_cursor, 0);
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

	/* free all chunks in list */
	if (!list_empty(&buf->chunk_list)) {
		c = list_get_first(&buf->chunk_list);
		while (c != &buf->chunk_list) {
			n = c->next;
			chunk = __get_trd_chunk_ptr(c);
			free(chunk);
			memstat_sub(&buf->mem_acc->ms, MEMSTAT_TRADE_DATA_BUFFER,
				sizeof(struct trade_data_buffer));
			c = n;
		}
	}

	free(buf);
}

/**
 * @brief   Push one trade record into buffer.
 *
 * @param   buf:       Buffer to push into.
 * @param   data:      Trade record to copy.
 * @param   free_list: Linked list pointer holding recycled chunks.
 *
 * @return  0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const trcache_trade_data *data,
	struct list_head *free_list)
{
	struct trade_data_chunk *tail = NULL, *new_chunk = NULL;

	if (!buf || !data) {
		return -1;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

	/*
	 * The user thread exclusively manages chunk allocation and maintains the
	 * free list. Whether a chunk has been fully consumed is determined by a
	 * counter incremented by the consumer threads. It’s possible for the
	 * consumer threads to be faster and consume all the way to the tail. In
	 * that case, if the consumer’s cursor is pointing to a chunk that has
	 * already been fully used, the user thread might place that chunk back into
	 * the free list. Later, when the consumer thread tries to advance its
	 * cursor, it could end up accessing a freed chunk. To prevent this, the
	 * user thread links a new chunk just before writing the last piece of data
	 * to ensure the consumer’s cursor never points to a fully consumed chunk.
	 */
	if ((tail->write_idx + 1) == NUM_TRADE_CHUNK_CAP) {
		if (free_list != NULL && !list_empty(free_list)) {
			list_move_tail(free_list->next, &buf->chunk_list);
			new_chunk = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));
		} else {
			new_chunk = malloc(sizeof(struct trade_data_chunk));
			if (new_chunk == NULL) {
				errmsg(stderr, "#trade_data_chunk allocation failed\n");
				return -1;
			}

			memstat_add(&buf->mem_acc->ms, MEMSTAT_TRADE_DATA_BUFFER,
				sizeof(struct trade_data_chunk));

			INIT_LIST_HEAD(&new_chunk->list_node);
			list_add_tail(&new_chunk->list_node, &buf->chunk_list);
		}

		buf->next_tail_write_idx = 0;

		atomic_store_explicit(&new_chunk->write_idx, 0,
			memory_order_release);
		atomic_store_explicit(&new_chunk->num_consumed_cursor, 0,
			memory_order_release);
	} else {
		buf->next_tail_write_idx += 1;
	}

	tail->entries[tail->write_idx] = *data;

	/*
	 * Consumers must access the last entry only after
	 * the new chunk has been linked.
	 */
	atomic_store_explicit(&tail->write_idx, tail->write_idx + 1,
		memory_order_release);

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
	struct trcache_trade_data **data_array,
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

	*data_array = &chunk->entries[cursor->peek_idx];
	*count = write_idx - cursor->peek_idx;

	/* advance peek cursor */
	cursor->peek_idx += *count;

	if (cursor->peek_idx == NUM_TRADE_CHUNK_CAP) {
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
 * @param   buf:       Buffer to reap the free chunks.
 * @param   free_list: Linked list pointer holding recycled chunks.
 */
void trade_data_buffer_reap_free_chunks(struct trade_data_buffer *buf,
	struct list_head *free_list)
{
	struct trade_data_chunk *tail = NULL, *chunk = NULL, *c = NULL;
	struct list_head *first = NULL, *last = NULL, *node = NULL, *next = NULL;

	if (free_list == NULL || list_empty(&buf->chunk_list)) {
		return;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

	first = list_get_first(&buf->chunk_list);

	chunk = __get_trd_chunk_ptr(first);

	/*
	 * Note that we should remain at least one node in the list.
	 * See the comment in the trade_data_buffer_push().
	 */
	while (chunk != tail) {
		if (atomic_load_explicit(&chunk->num_consumed_cursor,
				memory_order_acquire) != buf->num_cursor) {
			break;
		}

		last = (struct list_head *) &chunk->list_node;
		chunk = __get_trd_chunk_ptr(chunk->list_node.next);
	}

	if (last != NULL) {
		if (buf->mem_acc->aux_limit == 0 || 
			memstat_get_aux_total(&buf->mem_acc->ms) <= buf->mem_acc->aux_limit) {
			list_bulk_move_tail(free_list, first, last);
		} else {
			node = first;
			while (node != NULL) {
				c = __get_trd_chunk_ptr(node);
				next = (node == last) ? NULL : node->next;
				memstat_sub(&buf->mem_acc->ms, MEMSTAT_TRADE_DATA_BUFFER,
					sizeof(struct trade_data_chunk));
				free(c);
				if (node == last) {
					break;
				}
				node = next;
			}

			buf->chunk_list.next = &chunk->list_node;
			chunk->list_node.prev = &buf->chunk_list;
		}
	}
}
