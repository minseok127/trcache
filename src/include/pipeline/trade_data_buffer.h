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
	/*
	 * Group 1: Producer (Feed Thread) hot data.
	 * Written when pushing data or reaping chunks.
	 */
	____cacheline_aligned
	struct list_head list_node;
	_Atomic(int) write_idx;

	/*
	 * Group 2: Consumer (Apply Thread) hot data.
	 * Written by multiple consumers calling trade_data_buffer_consume().
	 */
	____cacheline_aligned
	_Atomic(int) num_consumed_cursor;

	/*
	 * Group 3: Data payload.
	 */
	____cacheline_aligned
	struct trcache_trade_data entries[NUM_TRADE_CHUNK_CAP];

} ____cacheline_aligned;

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
 * @in_use:        0 means the cursor is free; 1 means it is in use.
 *
 * Caller allocates this and passes to peek/consume.
 */
struct trade_data_buffer_cursor {
	struct trade_data_chunk	*consume_chunk;
	uint64_t consume_count;
	struct trade_data_chunk *peek_chunk;
	int peek_idx;
	_Atomic(int) in_use;
} __cacheline_aligned;

/*
 * trade_data_buffer - Buffer managing a linked list of trade_data_chunk.
 *
 * @chunk_list:          Linked list for chunks.
 * @produced_count:      Number of data items supplied to this buffer.
 * @next_tail_write_idx: Next write_idx of the tail chunk.
 * @num_cursor:          Number of valid cursors.
 * @trc:                 Back-pointer to the main trcache instance.
 * @symbol_id:           Integer symbol ID resolved via symbol table.
 * @memory_usage:        Memory (bytes) of this struct + all *active* chunks.
 * @cursor_arr:          Cursor array (only valid types are initialized).
 *
 * @next_tail_write_idx is a cached prediction of the tail chunk's write-index
 * after the very next push. It lets external code (caller side) decide *before*
 * calling trade_data_buffer_push() whether this push will require allocating /
 * linking a new chunk.
 */
struct trade_data_buffer {
	/*
	 * Group 1: Producer (Feed Thread) hot data.
	 * Written when pushing data or reaping chunks.
	 */
	____cacheline_aligned
	struct list_head chunk_list;
	_Atomic(uint64_t) produced_count;
	int next_tail_write_idx;

	/*
	 * Group 2: Read-mostly / Cold data.
	 * Initialized once, then read.
	 */
	____cacheline_aligned
	int num_cursor;
	struct trcache *trc;
	int symbol_id;

	/*
	 * Group 3: Memory Counter
	 * Written by producer thread, read by admin thread.
	 * Padded to prevent false sharing with cursors.
	 */
	____cacheline_aligned
	struct mem_padded_atomic_size memory_usage;

	/*
	 * Group 4: Consumer (Apply Thread) hot data area.
	 * This is an array of aligned cursors. Each cursor is written by
	 * a different Apply thread.
	 */
	____cacheline_aligned
	struct trade_data_buffer_cursor cursor_arr[MAX_CANDLE_TYPES];

} ____cacheline_aligned;

/**
 * @brief   Obtain a cursor positioned at the given type.
 *
 * @param   buf:         Pointer to the #trade_data_buffer holding cursor.
 * @param   candle_idx:  Desired candle type index.
 *
 * @return  Pointer of the #trade_data_buffer_cursor when it is free.
 */
static inline struct trade_data_buffer_cursor *trade_data_buffer_acquire_cursor(
	struct trade_data_buffer *buf, int candle_idx)
{
	struct trade_data_buffer_cursor *cur = &buf->cursor_arr[candle_idx];
	int expected = 0;

	if (atomic_load_explicit(&cur->in_use, memory_order_acquire) != 0) {
		return NULL;
	}

	if (atomic_compare_exchange_strong(&cur->in_use, &expected, 1)) {
		return cur;
	}

	return NULL;
}

/**
 * @brief   Release a previously acquired cursor.
 *
 * Callers must invoke this after they finish with the cursor returned from
 * trade_data_buffer_get_cursor().  It resets the in_use flag to 0.
 *
 * @param   cursor: Pointer to #trade_data_buffer_cursor acquired earlier.
 */
static inline void trade_data_buffer_release_cursor(
	struct trade_data_buffer_cursor *cursor)
{
	if (cursor != NULL) {
		atomic_store_explicit(&cursor->in_use, 0, memory_order_release);
	}
}

/**
 * @brief   Create and initialize a trade_data_buffer.
 *
 * @param   tc:         Pointer to the #trcache.
 * @param   symbol_id:  Integer symbol ID.
 *
 * @return  Pointer to buffer, or NULL on failure.
 */
struct trade_data_buffer *trade_data_buffer_init(struct trcache *tc,
	int symbol_id);

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
 * @param   thread_id: The feed thread's unique ID.
 *
 * @return  0 on success, -1 on error.
 */
int trade_data_buffer_push(struct trade_data_buffer *buf,
	const struct trcache_trade_data *data, struct list_head *free_list,
	int thread_id);

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
 * This function attempts to reclaim memory in several stages. First, it tries
 * to move fully consumed chunks to the 'free_list'. If the memory limit is
 * exceeded, it frees the chunks directly instead.
 *
 * @param   buf:                 Buffer to reap the free chunks.
 * @param   free_list:           Linked list pointer holding recycled chunks.
 * @param   thread_id:           The feed thread's unique ID.
 */
void trade_data_buffer_reap_free_chunks(struct trade_data_buffer *buf,
	struct list_head *free_list, int thread_id);

#endif /* TRADE_DATA_BUFFER_H */
