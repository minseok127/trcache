#ifndef CANDLE_CHUNK_LIST_H
#define CANDLE_CHUNK_LIST_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "concurrent/scalable_queue.h"
#include "pipeline/candle_chunk.h"
#include "pipeline/candle_chunk_index.h"
#include "utils/log.h"
#include "utils/memstat.h"

/*
 * candle_chunk_list_head_version - Covers the lifetime of the chunks.
 *
 * @head_version_prev:  Previously created head version.
 * @head_version_next:  Newly created head version.
 * @tail_node:          Most recent node covered by this head version.
 * @head:node:          Oldest node covered by this head version.
 *
 * #candle_chunks are managed as a linked list, and the head of the list is
 * managed in an RCU-like manner. This means that when moving the head,
 * intermediate chunks are not immediately freed but are given a grace period,
 * which is managed using the atomsnap.
 *
 * When the head is moved, a range of chunks covering the lifetime of the
 * previous head is created. If this range is at the end of the linked list, it
 * indicates that other threads no longer traverse this range of chunks.
 *
 * To verify this, each head version is also linked via a pointers, and this
 * pointers is used to determine whether the head version represents the last
 * range of the linked list.
 */
struct candle_chunk_list_head_version {
	_Atomic(struct candle_chunk_list_head_version *)head_version_prev;
	_Atomic(struct candle_chunk_list_head_version *)head_version_next;
	_Atomic(struct candle_chunk *)tail_node;
	struct candle_chunk *head_node;
};

/*
 * candle_chunk_list_init_ctx - All parameters required to create a chunk list.
 *
 * @trc:         #trcache back-pointer.
 * @candle_idx:  Candle type index.
 * @symbol_id:   Integer symbol ID resolved via symbol table.
 */
struct candle_chunk_list_init_ctx {
	struct trcache *trc;
	int candle_idx;
	int symbol_id;
};

/*
 * candle_chunk_list - List of chunks containing candles (row and column batch).
 *
 * @candle_mutable_chunk:    Chunk containing mutable candle.
 * @tail:                    Tail of the linked list where new chunks are added.
 * @mutable_seq:             Sequence number of mutable candle.
 * @ema_cycles_per_apply:    EMA cycles per single APPLY item.
 * @converting_chunk:        Chunk being converted to a column batch.
 * @last_seq_converted:      Highest seqnum already converted to COLUMN batch.
 * @ema_cycles_per_convert:  EMA cycles per single CONVERT item.
 * @head_gate:               Gate managing head versions.
 * @ema_cycles_per_flush:    EMA cycles per single FLUSH item.
 * @unflushed_batch_count:   Number of batches not yet flushed.
 * @config:                  Pointer to the configuration for this candle type.
 * @trc:                     #trcache back-pointer.
 * @chunk_index:             Chunk index based on sequence number and timestamp.
 * @row_page_count:          Number of row pages per chunk.
 * @candle_idx:              Candle type index.
 * @symbol_id:               Integer symbol ID resolved via symbol table.
 * @chunk_pool:              SCQ pool for recycling candle_chunk structs.
 * @row_page_pool:           SCQ pool for recycling candle_row_page structs.
 */
struct candle_chunk_list {
	/*
	 * Group 1: 'In-Memory' Worker (Apply + Convert) hot data.
	 * Written only by the worker that owns the Apply/Convert tasks.
	 *
	 * This layout is aligned with the scheduling logic, which tries to place
	 * 'Apply' and 'Convert' tasks for the same list onto a same core to
	 * maximize cache locality.
	 */
	____cacheline_aligned
	struct candle_chunk *candle_mutable_chunk;
	_Atomic(struct candle_chunk *)tail;
	_Atomic(uint64_t) mutable_seq;
	_Atomic(uint64_t) ema_cycles_per_apply;

	struct candle_chunk *converting_chunk;
	_Atomic(uint64_t) last_seq_converted;
	_Atomic(uint64_t) ema_cycles_per_convert;

	/*
	 * Group 2: 'I/O' Worker (Flush) hot data.
	 * Written only by the worker that owns the FLUSH task.
	 */
	____cacheline_aligned
	struct atomsnap_gate *head_gate;
	_Atomic(uint64_t) ema_cycles_per_flush;

	/*
	 * Group 3: Shared counter (True Sharing).
	 * Written by CONVERT (add) and FLUSH (sub) workers.
	 */
	____cacheline_aligned
	_Atomic(int) unflushed_batch_count;

	/*
	 * Group 4: Read-mostly / Cold data.
	 * Initialized once, then read frequently. No false-sharing risk.
	 */
	____cacheline_aligned
	const struct trcache_candle_config *config;
	struct trcache *trc;
	struct candle_chunk_index *chunk_index;
	int row_page_count;
	int candle_idx;
	int symbol_id;
	struct scalable_queue *chunk_pool;
	struct scalable_queue *row_page_pool;

} ____cacheline_aligned;

/**
 * @brief   Allocate and initialize the #candle_chunk_list.
 *
 * @param   ctx: Init context that contains parameters.
 *
 * @return  Pointer to #candle_chunk_list or NULL on failure.
 */
struct candle_chunk_list *create_candle_chunk_list(
	struct candle_chunk_list_init_ctx *ctx);

/**
 * @brief   Destroy all chunks.
 *
 * This function must be called only after all chunks in the list have been
 * converted to column batches.
 *
 * @param   chunk_list: Pointer from create_candle_chunk_list().
 */
void destroy_candle_chunk_list(struct candle_chunk_list *chunk_list);

/**
 * @brief    Apply trade data to the appropriate candle.
 *
 * @param    list:  Pointer to the candle chunk list.
 * @param    trade: Trade data to apply.
 *
 * @return   0 on success, or -1 if the trade data is not applied.
 *
 * Finds the corresponding candle in the chunk list and updates it with the
 * trade. Creates a new chunk if necessary.
 *
 * The admin thread must ensure that the apply function for a single chunk list
 * is executed by only one worker thread at a time.
 */
int candle_chunk_list_apply_trade(struct candle_chunk_list *list,
	void *trade);

/**
 * @brief    Convert all immutable row candles into a column batch.
 *
 * @param    list: Pointer to the candle chunk list.
 *
 * @return   Number of candles converted in this call.
 *
 * The admin thread must ensure that the convert function for a single chunk
 * list is executed by only one worker thread at a time.
 */
int candle_chunk_list_convert_to_column_batch(struct candle_chunk_list *list);

/**
 * @brief    Finalize the current mutable candle and convert all remaining
 *           candles to column format.
 *
 * @param    list: Pointer to the candle chunk list.
 */
void candle_chunk_list_finalize(struct candle_chunk_list *list);

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list:  Pointer to the candle chunk list.
 *
 * @return   The number of batches flushed in this call.
 *
 * May invoke user-supplied flush callbacks.
 *
 * The admin thread must ensure that the flush function for a single chunk
 * list is executed by only one worker thread at a time.
 */
int candle_chunk_list_flush(struct candle_chunk_list *list);

/**
 * @brief   Copy @count candles ending at @seq_end.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   seq_end:     Sequence number of the last candle.
 * @param   count:       Number of candles to copy.
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_list_copy_backward_by_seq(struct candle_chunk_list *list,
	uint64_t seq_end, int count, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request);

/**
 * @brief   Copy @count candles whose range ends at the candle
 *          with the specified @key.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   key:         Key belonging to the last candle.
 * @param   count:       Number of candles to copy.
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure.
 *
 * @note    If @key is outside the range of known candles, returns -1.
 */
int candle_chunk_list_copy_backward_by_key(struct candle_chunk_list *list,
	uint64_t key, int count, struct trcache_candle_batch *dst,
	const trcache_field_request *request);

/**
 * @brief   Copy candles within the key range [start_key, end_key].
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   start_key:   Key of the first candle (inclusive).
 * @param   end_key:     Key of the last candle (inclusive).
 * @param   dst:         Pre-allocated destination batch (SoA).
 * @param   request:     Specifies which user-defined fields to retrieve.
 *
 * @return  0 on success, -1 on failure (e.g., capacity insufficient).
 *
 * @note    If start_key or end_key is outside the available range, the query
 *          is clamped to the available bounds. If no candles fall within the
 *          range, dst->num_candles is set to 0 and returns 0.
 */
int candle_chunk_list_copy_by_key_range(struct candle_chunk_list *list,
	uint64_t start_key, uint64_t end_key, struct trcache_candle_batch *dst,
	const struct trcache_field_request *request);

/**
 * @brief   Count the number of candles within the key range [start_key, end_key].
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   start_key:   Key of the first candle (inclusive).
 * @param   end_key:     Key of the last candle (inclusive).
 *
 * @return  Number of candles in the range (>= 0), or -1 on failure.
 *
 * @note    If start_key or end_key is outside the available range, the count
 *          is based on the clamped bounds. Returns 0 if no candles exist.
 */
int candle_chunk_list_count_by_key_range(struct candle_chunk_list *list,
	uint64_t start_key, uint64_t end_key);

#endif /* CANDLE_CHUNK_LIST_H */

