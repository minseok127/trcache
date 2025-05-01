#ifndef TRADE_DATA_BUFFER_H
#define TRADE_DATA_BUFFER_H

#include <stddef.h>
#include <stdatomic.h>

#include "utils/list_head.h"

#include "trcache.h"

#ifndef __cacheline_aligned
#define __cacheline_aligned __attribute__((aligned(64)))
#endif /* __cacheline_aligned */

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
 * @list_node:           Linked list node
 * @write_idx:           Next write index [0..NUM_TRADE_CHUNK_CAP)
 * @num_consumed_cursor: Atomic count of totally consumed cursor
 * @entries:             Fixed array of data
 *
 * @note Entries are copied into the chunk; pointer ownership remains with
 *       the caller.
 */
struct trade_data_chunk {
	struct list_head list_node;
	_Atomic size_t write_idx;
	_Atomic size_t num_consumed_cursor;
	struct trcache_trade_data entries[NUM_TRADE_CHUNK_CAP];
};

#ifndef __get_trd_chunk_ptr
#define __get_trd_chunk_ptr(list_node_ptr) \
	list_entry(list_node_ptr, struct trade_data_chunk, list_node)
#endif /* __get_trd_chunk_ptr */
	

/*
 * trade_data_buffer_cursor - Cursor for iterating and consuming a buffer.
 *
 * @peek_chunk:    Chunk for next peek
 * @peek_idx:      Index in the peek_chunk for next peek
 * @consume_chunk: Chunk for next consume
 *
 * Caller allocates this and passes to peek/consume.
 */
struct trade_data_buffer_cursor {
	struct trade_data_chunk *peek_chunk;
	size_t peek_idx; 
	struct trade_data_chunk	*consume_chunk;
} __cacheline_aligned;

/*
 * trade_data_buffer - Buffer managing a linked list of trade_data_chunk.
 *
 * @chunk_list:         Linked list for chunks
 * @cursor_arr:         Cursor array
 * @num_cursor:         Number of cursors
 * @free_list:          Global linked list pointer holding recycled chunks
 */
struct trade_data_buffer {
	struct list_head chunk_list;
	struct trade_data_buffer_cursor *cursor_arr;
	size_t num_cursor;
	struct list_head *free_list;
};

/**
 * @brief Create a new trade data buffer.
 *
 * @param num_cursor: Number of cursors (candle types) tracked per trade.
 * @param free_list:  Free list pointer.
 *
 * @return Pointer to buffer, or NULL on allocation failure.
 */
struct trade_data_buffer *trade_data_buffer_init(int num_cursor,
	struct list_head *free_list);

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
 * @param buf:     Buffer to consume from.
 * @param cursor:  Pointer to cursor used in peek.
 */
void trade_data_buffer_consume(struct trade_data_buffer *buf,
	struct trade_data_buffer_cursor *cursor);

#endif /* TRADE_DATA_BUFFER_H */
