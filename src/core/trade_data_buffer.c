/**
 * @file   trade_data_buffer.c
 * @brief  Chunk‐based buffer for trcache_trade_data
 *
 * Producer pushes trade data into chunks; when full, obtains a free
 * chunk from free list or mallocs a new one. Consumers use a cursor to peek
 * and consume entries; when threshold reached, chunk is enqueued back
 * to free list by producer's context.
 */

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "utils/list_head.h"

#include "trade_data_buffer.h"

/**
 * @brief   Create and initialize a trade_data_buffer.
 *
 * @param num_cursor:     Number of cursors (candle types) to track
 *
 * @return pointer to buffer, or NULL on failure.
 */
struct trade_data_buffer *trade_data_buffer_init(int num_cursor)
{
	struct trade_data_buffer *buf = NULL;
	struct trade_data_chunk *chunk = NULL;
	struct trade_data_buffer_cursor *c = NULL;

	if (num_cursor == 0) {
		fprintf(stderr, "trade_data_buffer_init: num_cursor is 0\n");
		return NULL;
	}

	buf = malloc(sizeof(struct trade_data_buffer));

	if (buf == NULL) {
		fprintf(stderr, "trade_data_buffer_init: buffer malloc failed\n");
		return NULL;
	}

	buf->cursor_arr
		= malloc(num_cursor * sizeof(struct trade_data_buffer_cursor));
	
	if (buf->cursor_arr == NULL) {
		fprintf(stderr, "trade_data_buffer_init: cursor malloc failed\n");
		free(buf);
		return NULL;
	}

	chunk = malloc(sizeof(struct trade_data_chunk));

	if (chunk == NULL) {
		fprintf(stderr, "trade_data_buffer_init: chunk malloc failed\n");
		free(buf->cursor_arr);
		free(buf);
		return NULL;
	}

	INIT_LIST_HEAD(&buf->chunk_list);

	/* Add the first chunk into chunk list */
	INIT_LIST_HEAD(&chunk->list_node);
	atomic_store(&chunk->write_idx, 0);
	atomic_store(&chunk->num_consumed_cursor, 0);
	list_add_tail(&chunk->list_node, &buf->chunk_list);

	/* Init cursors */
	for (int i = 0; i < num_cursor; i++) {
		c = buf->cursor_arr + i;
		c->peek_chunk = chunk;
		c->peek_idx = 0;
		c->consume_chunk = chunk;
	}

	buf->num_cursor = num_cursor;

	buf->next_tail_write_idx = 0;

	return buf;
}

/**
 * @brief Destroy buffer and free all resources.
 *
 * @param buf: Buffer to destroy.
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
			c = n;
		}
	}

	free(buf->cursor_arr);
	free(buf);
}

/**
 * @brief   Push one trade record into buffer.
 *
 * @param buf:       Buffer to push into.
 * @param data:      Trade record to copy.
 * @param free_list: Linked list pointer holding recycled chunks.
 *
 * @return: 0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const trcache_trade_data *data,
	struct list_head *free_list)
{
	struct trade_data_chunk *tail = NULL, *new_chunk = NULL

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
				fprintf(stderr, "trade_data_buffer_push: malloc failed\n");
				return -1;
			}

			INIT_LIST_HEAD(&new_chunk->list_node);
			list_add_tail(&new_chunk->list_node, &buf->chunk_list);
		}

		atomic_store(&new_chunk->write_idx, 0);
		atomic_store(&new_chunk->num_consumed_cursor, 0);

		buf->next_tail_write_idx = 0;
	} else {
		buf->next_tail_write_idx += 1;
	}

	tail->entries[tail->write_idx] = *data;

	/*
	 * Consumers must access the last entry only after
	 * the new chunk has been linked.
	 */
	__sync_synchronize();

	atomic_store(&tail->write_idx, tail->write_idx + 1);

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
	size_t *count)
{
	struct trade_data_chunk *chunk = NULL;

	if (!buf || !cursor || !data_array || !count) {
		if (count) {
			*count = 0;
		}
		return 0;
	}

	assert(cursor->peek_chunk != NULL);
	chunk = cursor->peek_chunk;

	if (cursor->peek_idx == chunk->write_idx) {
		*count = 0;
		return 0;
	} 

	*data_array = &chunk->entries[cursor->peek_idx];
	*count = chunk->write_idx - cursor->peek_idx;

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
 * @brief	Consume all entries that the caller has already peeked.
 *
 * @param	buf:	buffer to consume from.
 * @param	cursor:	caller-managed cursor (same one used for peek).
 */
void trade_data_buffer_consume(struct trade_data_buffer	*buf,
	struct trade_data_buffer_cursor *cursor)
{
	struct trade_data_chunk *chunk = NULL;
	struct list_head *c = NULL;

	if (!buf || !cursor) {
		return;
	}

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
		__sync_synchronize();

		atomic_fetch_add(&chunk->num_consumed_cursor, 1);

		chunk = __get_trd_chunk_ptr(c);
	}

	cursor->consume_chunk = chunk;
}

/**
 * @brief   Move free chunks into the free list
 *
 * @param   buf:       Buffer to reap the free chunks.
 * @param   free_list: Linked list pointer holding recycled chunks.
 */
void trade_data_buffer_reap_free_chunks(struct trade_data_buffer *buf,
	struct list_head *free_list)
{
	struct trade_data_chunk *tail = NULL, *chunk = NULL;
	struct list_head *first = NULL, *last = NULL;

	if (free_list == NULL) {
		return;
	}

	tail = __get_trd_chunk_ptr(list_get_last(&buf->chunk_list));

	first = list_get_first(&buf->chunk_list);

	chunk = __get_trd_chunk_ptr(first);

	while (chunk != tail) {
		if (atomic_load(&chunk->num_consumed_cursor) != buf->num_cursor) {
			break;
		}

		last = (struct list_head *) &chunk->list_node;
		chunk = __get_trd_chunk_ptr(chunk->list_node.next);
	}

	if (last != NULL) {
		list_bulk_move_tail(free_list, first, last);
	}
}
