#ifndef CANDLE_CHUNK_H
#define CANDLE_CHUNK_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "concurrent/atomsnap.h"
#include "utils/memstat.h"

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
 * @flush_handle:         Returned pointer of trcache_batch_flush_ops->flush().
 * @next:                 Linked list pointer to the next chunk.
 * @prev:                 Linked list pointer to the previous chunk.
 * @row_gate:             atomsnap_gate for managing #candle_row_pages.
 * @column_batch:         Structure of Arrays (SoA) buffer
 * @seq_first:            First sequence number of the chunk.
 * @mem_acc:              Memory accounting information.
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
	struct memory_accounting *mem_acc;
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
 * @brief   Compute row index from a linear record index.
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
 * @brief    Map @seq into the index range of @chunk.
 *
 * @param    chunk:   Pointer to the candle_chunk.
 * @param    seq:     Absolute sequence number to clamp.
 *
 * @return   The linear record index in the chunk.
 */
static inline int candle_chunk_clamp_seq(
	struct candle_chunk *chunk, uint64_t seq)
{
	int last_idx = atomic_load_explicit(&chunk->num_completed,
		memory_order_acquire);

	if (seq < chunk->seq_first) {
		return 0;
	} else if (seq > chunk->seq_first + last_idx) {
		return last_idx;
	} else {
		return (int)(seq - chunk->seq_first);
	}
}

/**
 * @brief    Return the zero-based index of the candle whose range
 *           contains @ts inside @chunk.
 *
 * @param    chunk:      Pointer to the candle_chunk.
 * @param    target_ts:  Timestamp to locate.
 *
 * @return   The linear record index in the chunk, or -1 on out-of-range.
 */
static inline int candle_chunk_find_idx_by_ts(
	struct candle_chunk *chunk, uint64_t target_ts)
{
	int num_rows = atomic_load_explicit(&chunk->num_completed, 
		memory_order_acquire) + 1;
	const uint64_t *start_ts_arr = chunk->column_batch->start_timestamp_array;
	int lo = 0, hi = num_rows - 1, mid;

	if (target_ts < start_ts_arr[0] || target_ts > start_ts_arr[num_rows - 1]) {
		return -1;
	}

	while (lo < hi) {
		mid = lo + ((hi - lo + 1) >> 1);

		if (start_ts_arr[mid] <= target_ts) {
			lo = mid;
		} else {
			hi = mid - 1;
		}
	}

	return lo;
}

/**
 * @brief   Return the absolute sequence number of the candle whose range
 *          contains @ts inside @chunk.
 *
 * @param   chunk:      Pointer to the candle_chunk.
 * @param   target_ts:  Timestamp to locate.
 *
 * @return  Absolute sequence number on success, UINT64_MAX if @ts is
 *          before the first candle or after the last candle’s start.
 */
static inline uint64_t candle_chunk_calc_seq_by_ts(
	struct candle_chunk *chunk, uint64_t target_ts)
{
	int idx = candle_chunk_find_idx_by_ts(chunk, target_ts);

	if (idx == -1) {
		return UINT64_MAX;
	}

	return chunk->seq_first + idx;
}

/**
 * @brief   Convenience wrapper to write start timestamp array.
 *
 * @param   chunk:    Target chunk to write start timestamp.
 * @param   page_idx: Target candle's row page index.
 * @param   row_idx:  Target candle's row index within the row page.
 * @param   ts:       Start timestamp of the candle.
 */
static inline void candle_chunk_write_start_timestamp(
	struct candle_chunk *chunk, int page_idx, int row_idx, uint64_t ts)
{
	int record_idx = candle_chunk_calc_record_index(page_idx, row_idx);
	chunk->column_batch->start_timestamp_array[record_idx] = ts;
}

/**
 * @brief   Allocate and initialize #candle_chunk.
 *
 * @param   candle_type:        Candle type of the column-batch.
 * @param   symbol_id:          Symbol ID of the column-batch.
 * @param   row_page_count:     Number of row pages per chunk.
 * @param   batch_candle_count: Number of candles per chunk.
 * @param   mem_acc:            Memory accounting information.
 *
 * @return  Pointer to the candle_chunk, or NULL on failure.
 */
struct candle_chunk *create_candle_chunk(trcache_candle_type candle_type,
	int symbol_id, int row_page_count, int batch_candle_count,
	struct memory_accounting *mem_acc);

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
	const struct trcache_candle_update_ops *ops,
	struct trcache_trade_data *trade);

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
 * @param   flush_ops: User-defined batch flush operation callbacks.
 *
 * @return  1  flush completed synchronously  
 *          0  flush started asynchronously (still pending)  
 */
int candle_chunk_flush(struct trcache *trc, struct candle_chunk *chunk,
	const struct trcache_batch_flush_ops* flush_ops);

/**
 * @brief   Poll a candle chunk for flush completion.
 *
 * If the chunk was flushed synchronously (@chunk->is_flushed == 1) the
 * function returns immediately. Otherwise it queries the backend via
 * flush_ops->is_done(). When the backend signals completion, the flush
 * handle is destroyed and the chunk is marked flushed.
 *
 * @param   trc:       Pointer to the parent trcache instance.
 * @param   chunk:     Pointer to the target candle_chunk.
 * @param   flush_ops: User-defined batch flush operation callbacks.
 *
 * @return  1  flush has completed *in this call*.
 *          0  flush has not completed *in this call*.
 */
int candle_chunk_flush_poll(struct trcache *trc, struct candle_chunk *chunk,
	const struct trcache_batch_flush_ops* flush_ops);

/**
 * @brief   Copy a single mutable candle from a row page into a SoA batch.
 *
 * The function acquires the chunk's spin-lock, verifies that the target
 * record is still resident in a row page (i.e., has not yet been moved to
 * the column batch by the background *convert* thread), and copies the
 * requested fields into @dst.
 *
 * @param   chunk:       Pointer to the candle_chunk.
 * @param   idx:         Index of the candle to copy (0-based).
 * @param   dst_idx:     Index of the last element in @dst to fill.
 * @param   dst [out]:   Pre-allocated destination batch.
 * @param   field_mask:  Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_mutable_row(struct candle_chunk *chunk,
	int idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask);

/**
 * @brief   Copy a contiguous range of row-oriented candles.
 *
 * Starting at @end_record_idx and walks towards @start_record_idx (inclusive),
 * this routine copies all rows that are still in row pages into @dst.
 * If the background convert thread converts the range midway, the
 * function stops early and returns the count copied so far.
 *
 * @param   chunk:      Pointer to the candle_chunk.
 * @param   start_idx:  First candle index in the range (inclusive).
 * @param   end_idx:    Last candle index in the range (inclusive).
 * @param   dst_idx:    Index of the last element in @dst to fill.
 * @param   dst [out]:  Pre-allocated destination batch.
 * @param   field_mask: Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_rows_until_converted(struct candle_chunk *chunk,
	int start_idx, int end_idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask);

/**
 * @brief   Copy candles that already reside in the column batch.
 *
 * Unlike the previous helpers, this routine bypasses row pages entirely and
 * pulls data from the chunk-local columnar storage area. It therefore
 * requires that the specified record range has *already* been converted.
 *
 * @param   chunk:       Pointer to the candle_chunk.
 * @param   start_idx:   First candle index in the range (inclusive).
 * @param   end_idx:     Last candle index in the range (inclusive).
 * @param   dst_idx:     Index of the last element in @dst to fill.
 * @param   dst [out]:   Pre-allocated destination batch.
 * @param   field_mask:  Bit-mask describing which columns to copy.
 *
 * @return  The number of candles copied.
 */
int candle_chunk_copy_from_column_batch(struct candle_chunk *chunk,
	int start_idx, int end_idx, int dst_idx, struct trcache_candle_batch *dst,
	trcache_candle_field_flags field_mask);

#endif /* CANDLE_CHUNK_H */
