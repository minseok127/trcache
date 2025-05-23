#ifndef CANDLE_CHUNK_LIST_H
#define CANDLE_CHUNK_LIST_H

#include <errono.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "core/candle_chunk.h"
#include "core/candle_chunk_index.h"

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
	struct atomsnap_version *snap_version;
	struct candle_chunk_list_head_version *head_version_prev;
	struct candle_chunk_list_head_version *head_version_next;
	struct candle_chunk *tail_node;
	struct candle_chunk *head_node;
};

/*
 * candle_chunk_list_init_ctx - All parameters required to create a chunk list.
 *
 * @trc:          #trcache back-pointer.
 * @update_ops:   Candle update callbacks.
 * @candle_type:  Enum trcache_candle_type.
 * @symbol_id:    Integer symbol ID resolved via symbol table.
 */
struct candle_chunk_list_init_ctx {
	struct trcache *trc;
	struct candle_update_ops update_ops;
	trcache_candle_type candle_type;
	int symbol_id;
};

/*
 * candle_chunk_list - List of chunks containing candles (row and column batch).
 *
 * @mutable_seq:             Sequence number of mutable candle.
 * @last_seq_converted:      Highest seqnum already converted to COLUMN batch.
 * @unflushed_batch_count:   Number of batches not yet flushed.
 * @tail:                    Tail of the linked list where new chunks are added.
 * @candle_mutable_chunk:    Chunk containing mutable candle.
 * @converting_chunk:        Chunk being converted to a column batch.
 * @head_gate:               Gate managing head versions.
 * @update_ops:              Candle update callbacks.
 * @trc:                     #trcache back-pointer.
 * @chunk_index:             Chunk index based on sequence number and timestamp.
 * @row_page_count:          Number of row pages per chunk.
 * @candle_type:             Enum trcache_candle_type.
 * @symbol_id:               Integer symbol ID resolved via symbol table.
 */
struct candle_chunk_list {
	_Atomic uint64_t mutable_seq;
	_Atomic uint64_t last_seq_converted;
	_Atomic int unflushed_batch_count;
	struct candle_chunk *tail;
	struct candle_chunk *candle_mutable_chunk;
	struct candle_chunk *converting_chunk;
	struct atomsnap_gate *head_gate;
	struct candle_update_ops update_ops;
	struct trcache *trc;
	struct candle_chunk_index *chunk_index;
	int row_page_count;
	trcache_candle_type candle_type;
	int symbol_id;
};

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
	struct trcache_trade_data *trade);

/**
 * @brief    Convert all immutable row candles into a column batch.
 *
 * @param    list: Pointer to the candle chunk list.
 *
 * The admin thread must ensure that the convert function for a single chunk
 * list is executed by only one worker thread at a time.
 */
void candle_chunk_list_convert_to_column_batch(struct candle_chunk_list *list);

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list:  Pointer to the candle chunk list.
 *
 * May invoke user-supplied flush callbacks.
 *
 * The admin thread must ensure that the flush function for a single chunk
 * list is executed by only one worker thread at a time.
 */
void candle_chunk_list_flush(struct candle_chunk_list *list);

/**
 * @brief   Copy @count candles ending at @seq_end.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   seq_end:     Sequence number of the last candle.
 * @param   count:       Number of candles to copy.
 * @param   field_mask:  Bitmask representing trcache_candle_field_type flags.
 * @param   dst:         Pre-allocated destination batch (SoA).
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_list_copy_backward_by_seq(struct candle_chunk_list *list,
	uint64_t seq_end, int count, trcache_candle_field_flags field_mask,
	struct trcache_candle_batch *dst);

/**
 * @brief   Copy @count candles whose range ends at the candle
 *          that contains @ts_end.
 *
 * @param   list:        Pointer to the candle chunk list.
 * @param   ts_end:      Timestamp belonging to the last candle.
 * @param   count:       Number of candles to copy.
 * @param   field_mask:  Bitmask representing trcache_candle_field_type flags.
 * @param   dst:         Pre-allocated destination batch (SoA).
 *
 * @return  0 on success, -1 on failure.
 *
 * Last candle's start_ts <= @ts_end < next (last+1) candle's start_ts.
 */
int candle_chunk_list_copy_backward_by_ts(struct candle_chunk_list *list,
	uint64_t ts_end, int count, trcache_candle_field_flags field_mask,
	struct trcache_candle_batch *dst);

#endif /* CANDLE_CHUNK_LIST_H */

