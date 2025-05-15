#ifndef CANDLE_CHUNK_LIST_H
#define CANDLE_CHUNK_LIST_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "concurrent/atomsnap.h"
#include "trcache.h"

#ifndef TRCACHE_ROWS_PER_PAGE
#define TRCACHE_ROWS_PER_PAGE (64)  /* one 4 KiB page */
#endif

#ifndef TRCACHE_ROWS_PER_PAGE_SHIFT
#define TRCACHE_ROWS_PER_PAGE_SHIFT (6)
#endif

#ifndef TRCACHE_ROWS_PER_PAGE_MODULAR_MASK
#define TRCACHE_ROWS_PER_PAGE_MODULAR_MASK (TRCACHE_ROWS_PER_PAGE - 1)
#endif

/*
 * candle_row_page - 4 KiB array of row-oriented candles.
 *
 * @rows: Fixed AoS storage for TRCACHE_ROWS_PER_PAGE candles.
 */
struct candle_row_page {
	struct trcache_candle rows[TRCACHE_ROWS_PER_PAGE];
};

/*
 * candle_chunk - Owns a sequence window of candles.
 *
 * @spinlock:             Lock used to protect the candle being updated.
 * @num_completed:        Number of records (candles) immutable.
 * @num_converted:        Number of records (candles) converted to column batch.
 * @mutable_page_idx:     Page index of the candle being updated.
 * @mutable_row_idx:      Index of the row being updated within the page.
 * @converting_page_idx:  Index of the page being converted to column batch.
 * @converting_row_idx:   Index of the row being converted to column batch.
 * @next:                 Linked list pointer.
 * @row_gate:             atomsnap_gate with TRCACHE_NUM_ROW_PAGES slots.
 * @column_batch:         SoA buffer (NULL until first need).
 *
 * This structure is a linked list node of #candle_chunk_list. A Chunk holds
 * row-based candles initially, then converts them into columnar format for
 * efficient processing.
 */
struct candle_chunk {
	pthread_spinlock_t spinlock;
	_Atomic int num_completed;
	_Atomic int num_converted;
	int mutable_page_idx;
	int mutable_row_idx;
	int converting_page_idx;
	int converting_row_idx;
	struct candle_chunk *next;
	struct atomsnap_gate *row_gate;
	struct trcache_candle_batch *column_batch;
};

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
 * candle_update_ops - Callbacks for updating candles.
 *
 * @init:      Init function that sets up the candle with the first trade data.
 * @update:    Update function that reflects trade data.
 * @is_closed: Checks if trade data can be applied to the candle.
 */
struct candle_update_ops {
	void (*init)(struct trcache_candle *c, struct trcache_trade_data *d);
	void (*update)(struct trcache_candle *c, struct trcache_trade_data *d);
	bool (*is_closed)(struct trcache_candle *c, struct trcache_trade_data *d);
};

/*
 * candle_chunk_list_init_ctx - All parameters required to create a chunk list.
 *
 * @update_ops:              Candle update callbacks.
 * @flush_ops:               User-supplied callbacks used for flush.
 * @flush_threshold_batches: How many batches to buffer before flush.
 * @batch_candle_count:      Number of candles per column batch (chunk).
 * @candle_type:             Enum trcache_candle_type.
 * @symbol_id:               Integer symbol ID resolved via symbol table.
 */
struct candle_chunk_list_init_ctx {
	struct candle_update_ops update_ops;
	struct trcache_flush_ops flush_ops;
	int flush_threshold_batches;
	int batch_candle_count;
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
 * @flush_ops:               User-supplied callbacks used for flush.
 * @flush_threshold_batches: How many batches to buffer before flush.
 * @batch_candle_count:      Number of candles per column batch (chunk).
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
	struct trcache_flush_ops flush_ops;
	int flush_threshold_batches;
	int batch_candle_count;
	int row_page_count;
	trcache_candle_type candle_type;
	int symbol_id;
};

/**
 * @brief   Compute the linear index of a record in the chunk.
 *
 * @param   page_index: Index of the page.
 * @param   row_index:  Index of the row within the page.
 *
 * @return  The linear record index in the chunk assuming fixed rows per page.
 */
static inline int candle_chunk_calc_record_index(int page_index, int row_index)
{
	return (page_index << TRCACHE_ROWS_PER_PAGE_SHIFT) + row_index;
}

/**
 * @brief   Compute page index from a linear record index.
 *
 * @param   record_index: Linear index of the record in the chunk.
 *
 * @return  The page index in the chunk.
 */
static inline int candle_chunk_calc_page_idx(int record_index)
{
	return record_index >> TRCACHE_ROWS_PER_PAGE_SHIFT;
}

/**
 * @brief   Compute page index from a linear record index.
 *
 * @param   record_index: Linear index of the record in the chunk.
 *
 * @return  The row index in the page.
 */
static inline int candle_chunk_calc_row_idx(int record_index)
{
	return record_index & TRCACHE_ROWS_PER_PAGE_MODULAR_MASK;
}

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
 * @param    list: Pointer to the candle chunk list.
 *
 * May invoke user-supplied flush callbacks.
 *
 * The admin thread must ensure that the flush function for a single chunk
 * list is executed by only one worker thread at a time.
 */
void candle_chunk_list_flush(struct candle_chunk_list *list);

#endif /* CANDLE_CHUNK_LIST_H */

