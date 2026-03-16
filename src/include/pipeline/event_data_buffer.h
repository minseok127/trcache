#ifndef EVENT_DATA_BUFFER_H
#define EVENT_DATA_BUFFER_H

#include <stddef.h>
#include <stdatomic.h>

#include "meta/trcache_internal.h"
#include "utils/list_head.h"

#include "trcache.h"

/*
 * event_data_flush_ops - Generic flush ops for event_data_buffer.
 *
 * Both trcache_trade_flush_ops and trcache_book_event_flush_ops
 * share this exact layout (flush, is_done, ctx).
 */
struct event_data_flush_ops {
	void (*flush)(trcache *cache, int symbol_id,
		const void *io_block, int num_events,
		void *ctx);
	bool (*is_done)(trcache *cache,
		const void *io_block, void *ctx);
	void *ctx;
};

#ifndef __cacheline_aligned
#ifdef _MSC_VER
#define __cacheline_aligned __declspec(align(64))
#else
#define __cacheline_aligned __attribute__((aligned(64)))
#endif
#endif /* __cacheline_aligned */

/*
 * Alignment requirement for event block data buffers.
 *
 * 4 KiB matches the hardware page size and ensures that data pointers
 * passed to the flush callback are suitable for DMA transfers.
 */
#define EVENT_DATA_BUF_ALIGN (4096)

/*
 * event_block_flush_state - Lifecycle of a full event block's flush.
 *
 * NOT_READY:  The block has not yet been filled; no flush is needed.
 * NEEDED:     The block is full and flush() has not been called yet.
 * IN_FLIGHT:  flush() has been called; is_done() is being polled.
 * DONE:       The flush has completed (success or error).
 */
enum event_block_flush_state {
	EVENT_BLOCK_FLUSH_NOT_READY = 0,
	EVENT_BLOCK_FLUSH_NEEDED    = 1,
	EVENT_BLOCK_FLUSH_IN_FLIGHT = 2,
	EVENT_BLOCK_FLUSH_DONE      = 3,
};

/*
 * event_data_block - Single block storing multiple event entries.
 *
 * @list_node:           Linked list node.
 * @write_idx:           Next write index [0..events_per_block).
 * @data:                4 KiB-aligned buffer holding the raw event records.
 *                       Allocated separately at block creation and freed
 *                       when the block itself is freed. Preserved across
 *                       free-list recycling; never zeroed between reuses.
 * @num_consumed_cursor: Atomic count of totally consumed cursor.
 * @flush_state:         Current flush lifecycle state.
 *
 * @note data is copied into the block; pointer ownership remains with
 *       the caller.
 */
struct event_data_block {
	/*
	 * Group 1: Producer (Feed Thread) hot data.
	 * Written when pushing data or reaping blocks.
	 * data is set once at allocation and preserved for the block's
	 * lifetime, so it lives here alongside write_idx.
	 */
	____cacheline_aligned
	struct list_head list_node;
	_Atomic(int) write_idx;
	void *data;

	/*
	 * Group 2: Consumer (Apply Thread) hot data.
	 * Written by multiple consumers calling event_data_buffer_consume().
	 */
	____cacheline_aligned
	_Atomic(int) num_consumed_cursor;

	/*
	 * Group 3: Flush Worker exclusive data.
	 * Written once by the flush worker that owns this symbol.
	 */
	____cacheline_aligned
	_Atomic(int) flush_state;

} ____cacheline_aligned;

#ifndef __get_evt_block_ptr
#define __get_evt_block_ptr(list_node_ptr) \
	list_entry(list_node_ptr, struct event_data_block, list_node)
#endif /* __get_evt_block_ptr */

/*
 * event_data_buffer_cursor - Cursor for iterating and consuming a buffer.
 *
 * @consume_block: Block for next consume.
 * @consume_count: Number of data items consumed from this cursor.
 * @peek_block:    Block for next peek.
 * @peek_idx:      Index in the peek_block for next peek.
 * @in_use:        0 means the cursor is free; 1 means it is in use.
 *
 * Caller allocates this and passes to peek/consume.
 */
struct event_data_buffer_cursor {
	struct event_data_block	*consume_block;
	uint64_t consume_count;
	struct event_data_block *peek_block;
	int peek_idx;
	_Atomic(int) in_use;
} __cacheline_aligned;

/*
 * event_data_buffer - Buffer managing a linked list of event_data_block.
 *
 * @block_list:                Linked list for blocks.
 * @produced_count:            Number of data items supplied to this buffer.
 * @next_tail_write_idx:       Next write_idx of the tail block.
 * @num_cursor:                Number of valid cursors.
 * @trc:                       Back-pointer to the main trcache instance.
 * @symbol_id:                 Integer symbol ID resolved via symbol table.
 * @block_allocation_size:     Pre-calculated size of one block (struct + data).
 * @event_size:                Size of one event record in bytes.
 * @events_per_block:          Number of event records per block.
 * @event_buf_size:            4 KiB-aligned data buffer size per block.
 * @has_flush:                 True when a flush callback is configured for
 *                             this buffer. Controls whether full blocks are
 *                             marked EVENT_BLOCK_FLUSH_NEEDED on push.
 * @memory_usage:              Memory (bytes) of this struct + all *active*
 *                             blocks.
 * @num_full_unflushed_blocks: Number of full blocks whose flush has not yet
 *                             completed. Incremented by the producer when a
 *                             block fills; decremented by the flush worker
 *                             when flush completes.
 * @ema_cycles_per_flush:      EMA of CPU cycles per completed block flush.
 *                             Written by the flush worker; read by the admin
 *                             thread for scheduling cost estimation.
 * @cursor_arr:                Cursor array (only valid types are initialized).
 *
 * @next_tail_write_idx is a cached prediction of the tail block's write-index
 * after the very next push. It lets external code (caller side) decide *before*
 * calling event_data_buffer_push() whether this push will require allocating /
 * linking a new block.
 */
struct event_data_buffer {
	/*
	 * Group 1: Producer (Feed Thread) hot data.
	 * Written when pushing data or reaping blocks.
	 */
	____cacheline_aligned
	struct list_head block_list;
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
	size_t block_allocation_size;
	size_t event_size;
	int events_per_block;
	size_t event_buf_size;
	bool has_flush;

	/*
	 * Group 3: Memory Counter
	 * Written by producer thread, read by admin thread.
	 * Padded to prevent false sharing with cursors.
	 */
	____cacheline_aligned
	struct mem_padded_atomic_size memory_usage;

	/*
	 * Group 4: Flush accounting.
	 * num_full_unflushed_blocks is written by both the producer and the
	 * flush worker, so it lives on its own cache line.
	 */
	____cacheline_aligned
	_Atomic(int) num_full_unflushed_blocks;
	_Atomic(uint64_t) ema_cycles_per_flush;
	_Atomic(uint64_t) ema_cycles_per_update;

	/*
	 * Group 5: Consumer (Apply Thread) hot data area.
	 * This is an array of aligned cursors. Each cursor is written by
	 * a different Apply thread.
	 */
	____cacheline_aligned
	struct event_data_buffer_cursor cursor_arr[MAX_CANDLE_TYPES];

} ____cacheline_aligned;

/**
 * @brief   Obtain a cursor positioned at the given index.
 *
 * @param   buf:         Pointer to the #event_data_buffer holding cursor.
 * @param   cursor_idx:  Desired cursor index.
 *
 * @return  Pointer of the #event_data_buffer_cursor when it is free.
 */
static inline struct event_data_buffer_cursor *event_data_buffer_acquire_cursor(
	struct event_data_buffer *buf, int cursor_idx)
{
	struct event_data_buffer_cursor *cur = &buf->cursor_arr[cursor_idx];
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
 * event_data_buffer_acquire_cursor().  It resets the in_use flag to 0.
 *
 * @param   cursor: Pointer to #event_data_buffer_cursor acquired earlier.
 */
static inline void event_data_buffer_release_cursor(
	struct event_data_buffer_cursor *cursor)
{
	if (cursor != NULL) {
		atomic_store_explicit(&cursor->in_use, 0, memory_order_release);
	}
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
 * @param   has_flush:        True when a flush callback is configured.
 *
 * @return  Pointer to buffer, or NULL on failure.
 */
struct event_data_buffer *event_data_buffer_init(struct trcache *tc,
	int symbol_id, size_t event_size, int events_per_block,
	size_t event_buf_size, int num_cursors, bool has_flush);

/**
 * @brief   Destroy an event data buffer and free resources.
 *
 * @param   buf: Pointer returned by event_data_buffer_init.
 */
void event_data_buffer_destroy(struct event_data_buffer *buf);

/**
 * @brief   Push an event entry into the buffer.
 *
 * Copies the data into the tail block, allocating a new block if
 * the current one is full.
 *
 * @param   buf:       Buffer to push into.
 * @param   data:      Pointer to user-defined event data to copy.
 * @param   free_list: Linked list pointer holding recycled blocks.
 * @param   thread_id: The feed thread's unique ID.
 *
 * @return  0 on success, -1 on error.
 */
int event_data_buffer_push(struct event_data_buffer *buf,
	const void *data, struct list_head *free_list,
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
int event_data_buffer_peek(struct event_data_buffer *buf,
	struct event_data_buffer_cursor *cursor,
	void **data_array, int *count);

/**
 * @brief	Consume entries up to cursor's peek position.
 *
 * @param   buf:     Buffer to consume from.
 * @param   cursor:  Pointer to cursor used in peek.
 * @param   count:   Number of data items fetched via peek().
 */
void event_data_buffer_consume(struct event_data_buffer *buf,
	struct event_data_buffer_cursor *cursor, int count);

/**
 * @brief   Move free blocks into the free list.
 *
 * This function attempts to reclaim memory in several stages. First, it tries
 * to move fully consumed blocks to the 'free_list'. If the memory limit is
 * exceeded, it frees the blocks directly instead.
 *
 * A block is eligible for reclamation only when all cursors have
 * advanced past it AND, if flush is configured, its flush has completed.
 *
 * @param   buf:       Buffer to reap the free blocks.
 * @param   free_list: Linked list pointer holding recycled blocks.
 * @param   thread_id: The feed thread's unique ID.
 */
void event_data_buffer_reap_free_blocks(struct event_data_buffer *buf,
	struct list_head *free_list, int thread_id);

/**
 * @brief   Flush full event blocks using the provided flush callbacks.
 *
 * Iterates all blocks ahead of the current tail. For each block in the
 * NEEDED or IN_FLIGHT state, drives the flush to completion.
 * Completed blocks are marked DONE and num_full_unflushed_blocks
 * is decremented.
 *
 * The active tail block is never touched here; call
 * event_data_buffer_finalize() to flush it during shutdown.
 *
 * Must be called only by the flush worker that holds flush ownership
 * for this buffer's symbol.
 *
 * @param   buf:  Buffer whose blocks to flush.
 * @param   ops:  Flush callback operations.
 *
 * @return  Number of blocks whose flush completed in this call.
 */
int event_data_buffer_flush_full_blocks(struct event_data_buffer *buf,
	const struct event_data_flush_ops *ops);

/**
 * @brief   Flush all remaining event blocks, including any partial tail.
 *
 * Intended for use during trcache_destroy(). Marks the current tail block
 * as needing a flush if it contains any data, then busy-polls until all
 * blocks (full and partial) have completed their flush. The tail is
 * handled directly here because event_data_buffer_flush_full_blocks()
 * deliberately skips it during normal operation.
 *
 * @param   buf:  Buffer to finalize.
 * @param   ops:  Flush callback operations.
 */
void event_data_buffer_finalize(struct event_data_buffer *buf,
	const struct event_data_flush_ops *ops);

#endif /* EVENT_DATA_BUFFER_H */
