#ifndef CANDLE_CHUNK_H
#define CANDLE_CHUNK_H

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
 * @is_flushed:           Flag to indicate flush state.
 * @flush_handle:         Returned pointer of trcache_flush_ops->flush().
 * @next:                 Linked list pointer to the next chunk.
 * @prev:                 Linked list pointer to the previous chunk.
 * @row_gate:             atomsnap_gate for managing #candle_row_pages.
 * @column_batch:         Structure of Arrays (SoA) buffer
 * @seq_first:            First sequence number of the chunk.
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
	int is_flushed;
	void *flush_handle;
	struct candle_chunk *next;
	struct candle_chunk *prev;
	struct atomsnap_gate *row_gate;
	struct trcache_candle_batch *column_batch;
	uint64_t seq_first;
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
 * @brief   Allocate and initialize #candle_chunk.
 *
 * @param   candle_type:        Candle type of the column-batch.
 * @param   symbol_id:          Symbol ID of the column-batch.
 * @param   row_page_count:     Number of row pages per chunk.
 * @param   batch_candle_count: Number of candles per chunk.
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
struct candle_chunk *create_candle_chunk(trcache_candle_type candle_type,
	int symbol_id, int row_page_count, int batch_candle_count);

/**
 * @brief   Release all resources of a candle chunk.
 *
 * @param   chunk: Candle-chunk pointer.
 */
void candle_chunk_destroy(struct candle_chunk *chunk);

/**
 * @brief   Initialize a row page within a candle chunk.
 *
 * @param   chunk:     Pointer to the candle chunk.
 * @param   page_idx:  Index of the page to initialize.
 * @param   ops:       Callback operations for candle initialization.
 * @param   trade:     First trade data used to initialize the first candle.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_page_init(struct candle_chunk *chunk, int page_idx,
	struct candle_update_ops *ops, struct trcache_trade_data *trade);

/**
 * @brief   Convert all immutable row candles within the given chunk.
 *
 * @param   chunk:     Target chunk to convert.
 * @param   start_idx: Start record index to convert.
 * @param   end_idx:   End record index to convert.
 */
void candle_chunk_convert_to_batch(struct candle_chunk *chunk,
	int start_idx, int end_idx);

/**
 * @brief   Flush a single fully-converted candle chunk.
 *
 * Starts a backend-specific flush on @chunk using the callbacks in
 * @trc->flush_ops. If the backend returns a non-NULL handle, the flush is
 * assumed to be asynchronous and remains "in-flight"; the caller must poll
 * it later with flush_ops->is_done(). When the backend performs a
 * synchronous flush it returns NULL, in which case the chunk is marked
 * immediately as flushed.
 *
 * @param   trc:       Pointer to the parent trcache instance.
 * @param   chunk:     Pointer to the target candle_chunk.
 *
 * @return  1  flush completed synchronously  
 *          0  flush started asynchronously (still pending)  
 */
int candle_chunk_flush(struct trcache *trc, struct candle_chunk *chunk);

/**
 * @brief   Poll a candle chunk for flush completion.
 *
 * If the chunk was flushed synchronously (@chunk->is_flushed == 1) the
 * function returns immediately. Otherwise it queries the backend via
 * flush_ops->is_done(). When the backend signals completion, the flush
 * handle is destroyed and the chunk is marked flushed.
 *
 * @param   trc:    Pointer to the parent trcache instance.
 * @param   chunk:  Pointer to the target candle_chunk.
 *
 * @return  1  flush has completed *in this call*.
 *          0  flush has not completed *in this call*.
 */
int candle_chunk_flush_poll(struct trcache *trc, struct candle_chunk *chunk);

#endif /* CANDLE_CHUNK_H */
