#ifndef TRADE_DATA_BUFFER_H
#define TRADE_DATA_BUFFER_H

#include <stddef.h>
#include <stdatomic.h>

#include "concurrent/scalable_queue.h"

#include "trcache.h"

/**
 * @brief Number of entries per trade data chunk.
 *
 * Can be overridden at compile time.
 */
#ifndef NUM_TRADE_CHUNK_CAP
#define NUM_TRADE_CHUNK_CAP (64)
#endif

/*
 * trade_data_chunk - Single chunk storing multiple trade entries.
 *
 * @next:          Next chunk in list
 * @write_idx:     Next write index [0..NUM_TRADE_CHUNK_CAP)
 * @consume_count: Atomic count of total consumed data
 * @entries:       Fixed array of data
 *
 * @note Entries are copied into the chunk; pointer ownership remains with
 *       the caller.
 */
struct trade_data_chunk {
	struct trade_data_chunk *next;
	size_t write_idx;
	_Atomic size_t consume_count;
	struct trcache_trade_data entries[NUM_TRADE_CHUNK_CAP];
};

/*
 * trade_data_buffer_cursor - Cursor for iterating and consuming a buffer.
 *
 * @peek_chunk:    Chunk for next peek
 * @peek_idx:      Index in the peek_chunk for next peek
 * @consume_chunk: Chunk for next consume
 * @consume_idx:   Index in the consume_chunk for next consume
 *
 * Caller allocates this and passes to peek/consume.
 */
struct trade_data_buffer_cursor {
	struct trade_data_chunk *peek_chunk;
	size_t peek_idx; 
	struct trade_data_chunk	*consume_chunk;
	size_t consume_idx; 
};

/*
 * trade_data_buffer - Buffer managing a linked list of trade_data_chunk.
 *
 * @head:               Chunk to read from
 * @tail:               Chunk to write to
 * @consume_threshold:  Total reads before recycling a chunk
 * @free_chunk_scq:     SCQ holding recycled chunks
 */
struct trade_data_buffer {
	trade_data_chunk *head;
	trade_data_chunk *tail;
	size_t consume_threshold;
	struct scalable_queue *free_chunk_scq;
};

/**
 * @brief Create a new trade data buffer.
 *
 * @param candle_count: Number of candle types tracked per trade.
 *
 * @return Pointer to buffer, or NULL on allocation failure.
 */
struct trade_data_buffer *trade_data_buffer_create(int candle_type_count);

/**
 * @brief Destroy a trade data buffer and free resources.
 *
 * @param buf: Pointer returned by trade_data_buffer_create.
 */
void trade_data_buffer_destroy(struct trade_data_buffer *buf);

/**
 * @brief Push a trade entry into the buffer.
 *
 * Copies the data into the tail chunk, allocating a new chunk if
 * the current one is full.
 *
 * @param buf:  Buffer to push into.
 * @param data: Pointer to trcache_trade_data to copy.
 *
 * @return 0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const struct trcache_trade_data *data);

/**
 * @brief	Peek at next entries for a given cursor.
 *
 * @param buf:	            Buffer to peek from.
 * @param cursor:	        Pointer to initialized cursor.
 * @param data_array (out): Pointer to entries array.
 * @param count (out):      Number of entries available from cursor.
 *
 * @return	1 if available, 0 if empty or on error.
 */
int trade_data_buffer_peek(struct trade_data_buffer *buf,
	struct trade_data_buffer_cursor *cursor,
	struct trcache_trade_data **data_array, size_t *count);

/**
 * @brief	Consume entries up to cursor’s peek position.
 *
 * Advances consume cursor; when a chunk’s total
 * consume_count reaches threshold, returns chunk to pool.
 *
 * @param buf:     Buffer to consume from.
 * @param cursor:  Pointer to cursor used in peek.
 */
void trade_data_buffer_consume(struct trade_data_buffer *buf,
	struct trade_data_buffer_cursor *cursor);

#endif /* TRADE_DATA_BUFFER_H */
