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

	/* allocate initial chunk */
	struct trade_data_chunk *chunk = malloc(sizeof(struct trade_data_chunk));
	if (chunk == NULL) {
		fprintf(stderr, "trade_data_buffer_create: chunk malloc failed\n");
		free(buf);
		return NULL;
	}

	chunk->next = NULL;
	chunk->write_idx = 0;
	atomic_init(&chunk->consume_count, 0);

	buf->head = chunk;
	buf->tail = chunk;
	buf->consume_threshold = NUM_TRADE_CHUNK_CAP * candle_type_count;

	buf->free_chunk_scq = scq_init();
	if (buf->free_chunk_scq == NULL) {
		fprintf(stderr, "trade_data_buffer_create: scq_init failed\n");
		free(chunk);
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
	struct size_t *count)
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

	if (cursor->peek_chunk && cursor->peek_idx >= NUM_TRADE_CHUNK_CAP) {
		if (cursor->peek_chunk->next) {
			cursor->peek_chunk = cursor->peek_chunk->next;
			cursor->peek_idx = 0;
		} else { /* no further chunks yet, dont move */
			*count = 0;
			return 0;
		}
	}

	assert(cursor->peek_chunk != NULL && cursor->peek_idx <= chunk->write_idx);
	chunk = cursor->peek_chunk;

	/* no new data in current chunk */
	if (cursor->peek_idx == chunk->write_idx) {
		*count = 0;
		return 0;
	}

	*data_array = &chunk->entries[cursor->peek_idx];
	*count = chunk->write_idx - cursor->peek_idx;

	/* advance peek cursor */
	cursor->peek_idx += *count;

	if (cursor->peek_idx >= NUM_TRADE_CHUNK_CAP && chunk->next) {
		cursor->peek_chunk = chunk->next;
		cursor->peek_idx = 0;
	} /* else; dont move */

	return 1;
}
