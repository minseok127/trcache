/**
 * @file   trade_data_buffer.c
 * @brief  Chunk‐based buffer for trcache_trade_data with SCQ reuse.
 *
 * Producer pushes trade data into chunks; when full, obtains a free
 * chunk from SCQ or mallocs a new one. Consumer uses a cursor to peek
 * and consume entries; when threshold reached, chunk is enqueued back
 * to SCQ for reuse.
 */

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "trade_data_buffer.h"

/**
 * @brief   Create and initialize a trade_data_buffer.
 *
 * @param candle_type_count: Number of candle types to track.
 *
 * @return pointer to buffer, or NULL on failure.
 */
struct trade_data_buffer *trade_data_buffer_create(size_t candle_type_count)
{
	struct trade_data_buffer *buf = malloc(sizeof(*buf));
	if (buf == NULL) {
		fprintf(stderr, "trade_data_buffer_create: malloc failed\n");
		return NULL;
	}

	// 이거 head랑 tail을 어케해야하지? 워커들과 유저스레드가 같은 헤드 테일에
	// 있으면?
	buf->head = NULL;
	buf->tail = NULL;
	buf->consume_threshold = NUM_TRADE_CHUNK_CAP * candle_type_count;

	buf->free_chunk_scq = scq_init();
	if (buf->free_chunk_scq == NULL) {
		fprintf(stderr, "trade_data_buffer_create: scq_init failed\n");
		free(buf);
		return NULL;
	}

	return buf;
}

/**
 * @brief Destroy buffer and free all resources.
 *
 * @param buf: Buffer to destroy.
 */
void trade_data_buffer_destroy(struct trade_data_buffer *buf)
{
	struct trade_data_chunk *c = NULL, *next = NULL, *rc = NULL;

	if (buf == NULL) {
		return;
	}

	/* free all chunks in list */
	c = buf->head;
	while (c) {
		next = c->next;
		free(c);
		c = next;
	}

	/* drain SCQ free list */
	struct trade_data_chunk *rc = NULL;
	while (scq_dequeue(buf->free_chunk_scq, (uint64_t *)&rc)) {
		free(rc);
	}

	scq_destroy(buf->free_chunk_scq);
	free(buf);
}

/**
 * @brief   Push one trade record into buffer.
 *
 * @param buf:  Buffer to push into.
 * @param data: Trade record to copy.
 *
 * @return: 0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const trcache_trade_data *data)
{
	struct trade_data_chunk *chunk = NULL, *new_chunk = NULL;

	if (!buf || !data) {
		return -1;
	}

	chunk = buf->tail;

	if (chunk->write_idx == NUM_TRADE_CHUNK_CAP) {
		/* obtain a free chunk or allocate */
		if (scq_dequeue(buf->free_chunk_scq, (uint64_t *)&new_chunk)) {
			new_chunk->next = NULL;
			new_chunk->write_idx = 0;
			atomic_store(&new_chunk->consume_count, 0);
		} else {
			new_chunk = malloc(sizeof(struct trade_data_chunk));
			if (new_chunk == NULL) {
				fprintf(stderr, "trade_data_buffer_push: malloc failed\n");
				return -1;
			}
			new_chunk->next = NULL;
			new_chunk->write_idx = 0;
			atomic_init(&new_chunk->consume_count, 0);
		}
		chunk->next = new_chunk;
		buf->tail = new_chunk;
		chunk = new_chunk;
	}

	/* copy data */
	chunk->entries[chunk->write_idx++] = *data;
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
		return 0;
	}

	/* Initial state */
	if (cursor->peek_chunk == NULL) {
		cursor->peek_chunk = buf->head;
		cursor->peek_idx = 0;
	}

	chunk = cursor->peek_chunk;
	assert(cursor->peek_idx <= chunk->write_idx);

	if (cursor->peek_idx == chunk->write_idx) {
		if (cursor->peek_idx == NUM_TRADE_CHUNK_CAP && chunk->next != NULL) {
			cursor->peek_chunk = chunk->next;
			cursor->peek_idx = 0;
			chunk = chunk->next;
			assert(chunk->write_idx != 0);
		} else {
			*count = 0;
			return 0;
		}
	} 

	*data_array = &chunk->entries[cursor->peek_idx];
	*count = chunk->write_idx - cursor->peek_idx;

	/* advance peek cursor */
	cursor->peek_idx += *count;

	return 1;
}

/**
 * @brief	Consume all entries that the caller has already peeked.
 *
 *	When a chunk’s global consume_count reaches
 *	    buf->consume_threshold
 *	it is considered fully processed by every candle type and is
 *	recycled into the SCQ free-list.
 *
 * @param	buf:	buffer to consume from.
 * @param	cursor:	caller-managed cursor (same one used for peek).
 */
void trade_data_buffer_consume(struct trade_data_buffer	*buf,
	struct trade_data_buffer_cursor *cursor)
{
	struct trade_data_chunk *chunk = NULL;
	size_t start, end, consume_count;

	if (!buf || !cursor || !cursor->peek_chunk) {
		return;
	}

	/* Initial state */
	if (cursor->consume_chunk == NULL) {
		cursor->consume_chunk = buf->head;
		cursor->consume_idx = 0;
	}

	/* nothing new to consume? */
	if (cursor->consume_chunk == cursor->peek_chunk &&
		cursor->consume_idx == cursor->peek_idx) {
		return;
	}

	/* loop until we catch up with peek position */
	chunk = cursor->consume_chunk;
	do {
		start = cursor->consume_idx;

		if (chunk == cursor->peek_chunk) {
			end = cursor->peek_idx;
		} else {
			end = NUM_TRADE_CHUNK_CAP;
		}

		if (end > start) {
			consume_count = atomic_fetch_add(&chunk->consume_count, end - start)
				+ (end - start);

			/* advance consume cursor inside this chunk */
			cursor->consume_idx = end;
		}

		if (chunk == cursor->peek_chunk) {
			break;
		}

		/* chunk fully consumed by this cursor: move to next */
		cursor->consume_chunk = chunk->next;
		cursor->consume_idx   = 0;

		/* was this the last candle-type to finish the chunk? */
		if (atomic_load(&chunk->consume_count) == buf->consume_threshold) {

			/* unlink only if chunk is still head and there is
			   another chunk behind it — keep at least one chunk
			   alive so producer has a target to write */
			if (buf->head == chunk && chunk->next) {
				buf->head = chunk->next;
			}
			/* recycle into SCQ */
			scq_enqueue(buf->free_chunk_scq,
				    (uint64_t)chunk);
		}
	} while (chunk != NULL);

	return 0;
}1
