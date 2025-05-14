#ifndef CANDLE_CHUNK_LIST_H
#define CANDLE_CHUNK_LIST_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "concurrent/atomsnap.h"
#include "trcache.h"

/* ---- Tunables -------------------------------------------------------- */

#ifndef TRCACHE_ROWS_PER_PAGE
#define TRCACHE_ROWS_PER_PAGE (64)  /* one 4 KiB page       */
#endif

#ifndef TRCACHE_CHUNK_CAP
#define TRCACHE_CHUNK_CAP (4096)    /* total rows per chunk */
#endif

#define TRCACHE_NUM_ROW_PAGES (TRCACHE_CHUNK_CAP / TRCACHE_ROWS_PER_PAGE)

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
 * @next:                 Linked list pointer.
 * @seq_begin:            First sequence number (inclusive).
 * @seq_end:              Last  +1  (exclusive).
 * @row_gate:             atomsnap_gate with TRCACHE_NUM_ROW_PAGES slots.
 * @column_batch:         SoA buffer (NULL until first need).
 * @mutable_page_index:   Page index of the candle being updated.
 * @mutable_row_index:    Index of the row being updated within the page.
 * @converting_page_idx:  Index of the page being converted to a column batch.
 * @converting_row_idx:   Index of the row being converted to a column batch.
 *
 * This structure is a linked list node of #candle_chunk_list.
 */
struct candle_chunk {
	struct candle_chunk *next;
	uint64_t seq_begin; /* inclusive */
	uint64_t seq_end;   /* exclusive */
	struct atomsnap_gate *row_gate;
	struct trcache_candle_batch *column_batch;
	int mutable_page_index;
	int mutable_row_index;
	int converting_page_idx;
	int converting_row_idx;
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
 * @symbol_id:               Integer symbol ID resolved via symbol table.
 * @candle_type:             Enum trcache_candle_type.
 */
struct candle_chunk_list_init_ctx {
	struct candle_update_ops update_ops;
	struct trcache_flush_ops flush_ops;
	uint32_t flush_threshold_batches;
	uint32_t batch_candle_count;
	int symbol_id;
	trcache_candle_type candle_type;
};

/*
 * candle_chunk_list -
 *
 * @tail:                    Tail of the linked list where new chunks are added.
 * @head_gate:               Gate managing head versions.
 * @candle_mutable_chunk:    Chunk containing mutable candle.
 * @converting_chunk:        Chunk being converted to a column batch.
 * @update_ops:              Candle update callbacks.
 * @flush_ops:               User-supplied callbacks used for flush.
 * @flush_threshold_batches: How many batches to buffer before flush.
 * @batch_candle_count:      Number of candles per column batch (chunk).
 * @symbol_id:               Integer symbol ID resolved via symbol table.
 * @candle_type:             Enum trcache_candle_type.
 * @last_row_completed:      Highest seqnum whose row is finished (immutable).
 * @last_row_converted:      Highest seqnum already converted to COLUMN batch.
 * @unflushed_batch_count:   Number of batches not yet flushed.
 * @spinlock:                Lock used to protect the candle being updated.
 */
struct candle_chunk_list {
	struct candle_chunk *tail;
	struct atomsnap_gate *head_gate;
	struct candle_chunk *candle_mutable_chunk;
	struct candle_chunk *converting_chunk;
	struct candle_update_ops update_ops;
	struct trcache_flush_ops flush_ops;
	uint32_t flush_threshold_batches;
	uint32_t batch_candle_count;
	int symbol_id;
	trcache_candle_type candle_type;
	_Atomic uint64_t last_row_completed;
	_Atomic uint64_t last_row_converted;
	_Atomic uint32_t unflushed_batch_count;
	pthread_spinlock_t spinlock;
};

/**
 * @brief   Translate @seq into a row-page slot index.
 *
 * @return  0 ... TRCACHE_NUM_ROW_PAGES-1
 */
static inline int candle_chunk_page_idx(const struct candle_chunk *chunk,
	uint64_t seq)
{
	return (int)((seq - chunk->seq_begin) / TRCACHE_ROWS_PER_PAGE);
}

/**
 * @brief   Allocate and initialize the #candle_chunk_list.
 *
 * @param   ctx: Init context that contains parameters.
 *
 * @return  Pointer to candle_chunk_list or NULL on failure.
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
 * @param    list:  Pointer to the candle chunk list
 * @param    trade: Trade data to apply
 *
 * Finds the corresponding candle in the chunk list and updates it with the
 * trade. Creates a new chunk if necessary.
 */
void candle_chunk_list_apply_trade(struct candle_chunk_list *list,
	struct trcache_trade_data *trade);

/**
 * @brief    Convert all mutable row candles into a column batch.
 *
 * @param    list: Pointer to the candle chunk list
 *
 * Transforms finalized row-based candles into columnar format.
 */
void candle_chunk_list_convert_to_column_batch(struct candle_chunk_list *list);

/**
 * @brief    Flush finalized column batches from the chunk list.
 *
 * @param    list: Pointer to the candle chunk list
 *
 * May invoke user-supplied flush callbacks.
 */
void candle_chunk_list_flush(struct candle_chunk_list *list);

#endif /* CANDLE_CHUNK_LIST_H */

