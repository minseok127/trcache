#ifndef TRADE_DATA_BUFFER_H
#define TRADE_DATA_BUFFER_H

#include <stddef.h>
#include <stdatomic.h>

#include "meta/trcache_internal.h"
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
 * @list_node:           Linked list node.
 * @write_idx:           Next write index [0..NUM_TRADE_CHUNK_CAP).
 * @num_consumed_cursor: Atomic count of totally consumed cursor.
 * @entries:             Fixed array of data.
 *
 * @note Entries are copied into the chunk; pointer ownership remains with
 *       the caller.
 */
struct trade_data_chunk {
	struct list_head list_node;
	_Atomic int write_idx;
	_Atomic int num_consumed_cursor;
	struct trcache_trade_data entries[NUM_TRADE_CHUNK_CAP];
};

#ifndef __get_trd_chunk_ptr
#define __get_trd_chunk_ptr(list_node_ptr) \
	list_entry(list_node_ptr, struct trade_data_chunk, list_node)
#endif /* __get_trd_chunk_ptr */
	

/*
 * trade_data_buffer_cursor - Cursor for iterating and consuming a buffer.
 *
 * @consume_chunk: Chunk for next consume.
 * @consume_count: Number of data items consumed from this cursor.
 * @peek_chunk:    Chunk for next peek.
 * @peek_idx:      Index in the peek_chunk for next peek.
 *
 * Caller allocates this and passes to peek/consume.
 */
struct trade_data_buffer_cursor {
	struct trade_data_chunk	*consume_chunk;
	uint64_t consume_count;
	struct trade_data_chunk *peek_chunk;
	int peek_idx; 
} __cacheline_aligned;

/*
 * trade_data_buffer - Buffer managing a linked list of trade_data_chunk.
 *
 * @chunk_list:          Linked list for chunks.
 * @produced_count:      Number of data items supplied to this buffer.
 * @cursor_arr:          Cursor array (only valid types are initialized).
 * @num_cursor:          Number of valid cursors.
 * @next_tail_write_idx: Next write_idx of the tail chunk.
 *
 * @next_tail_write_idx is a cached prediction of the tail chunk's write-index
 * after the very next push. It lets external code (caller side) decide *before*
 * calling trade_data_buffer_push() whether this push will require allocating /
 * linking a new chunk.
 */
struct trade_data_buffer {
	struct list_head chunk_list;
	uint64_t produced_count;
	struct trade_data_buffer_cursor cursor_arr[TRCACHE_NUM_CANDLE_TYPE];
	int num_cursor;
	int next_tail_write_idx;
};

/**
 * @brief   Obtain a cursor positioned at the given type.
 *
 * @param   buf:          Pointer to the #trade_data_buffer holding cursor.
 * @param   candle_type:  Desired candle type.
 *
 * @return  Pointer of the #trade_data_buffer_cursor (non-NULL).
 *
 * Validation of the type is the caller's responsibility.
 */
static inline struct trade_data_buffer_cursor *trade_data_buffer_get_cursor(
	struct trade_data_buffer *buf, trcache_candle_type candle_type)
{
	int bit = __builtin_ctz(candle_type);
	return (buf->cursor_arr + bit);
}

/**
 * @brief   Create and initialize a trade_data_buffer.
 *
 * @param   tc: Pointer to the #trcache.
 *
 * @return  Pointer to buffer, or NULL on failure.
 */
struct trade_data_buffer *trade_data_buffer_init(struct trcache *tc);

/**
 * @brief   Destroy a trade data buffer and free resources.
 *
 * @param   buf: Pointer returned by trade_data_buffer_create.
 */
void trade_data_buffer_destroy(struct trade_data_buffer *buf);

/**
 * @brief   Push a trade entry into the buffer.
 *
 * Copies the data into the tail chunk, allocating a new chunk if
 * the current one is full.
 *
 * @param   buf:       Buffer to push into.
 * @param   data:      Pointer to trcache_trade_data to copy.
 * @param   free_list: Linked list pointer holding recycled chunks.
 *
 * @return  0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const struct trcache_trade_data *data,
	struct list_head *free_list);

/**
 * @brief	Peek at next entries for a given cursor.
 *
 * @param   buf:	           Buffer to peek from.
 * @param   cursor:            Pointer to initialized cursor.
 * @param   data_array (out):  Pointer to entries array.
 * @param   count (out):       Number of entries available from cursor.
 *
 * @return	1 if available, 0 if empty or on error.
 */
int trade_data_buffer_peek(struct trade_data_buffer *buf,
	struct trade_data_buffer_cursor *cursor,
	struct trcache_trade_data **data_array, int *count);

/**
 * @brief	Consume entries up to cursor’s peek position.
 *
 * @param   buf:     Buffer to consume from.
 * @param   cursor:  Pointer to cursor used in peek.
 * @param   count:   Number of data items fetched via peek().
 */
void trade_data_buffer_consume(struct trade_data_buffer *buf,
	struct trade_data_buffer_cursor *cursor, int count);

/**
 * @brief   Move free chunks into the free list.
 *
 * @param   buf:       Buffer to reap the free chunks.
 * @param   free_list: Linked list pointer holding recycled chunks.
 */
void trade_data_buffer_reap_free_chunks(struct trade_data_buffer *buf,
	struct list_head *free_list);

#endif /* TRADE_DATA_BUFFER_H */
