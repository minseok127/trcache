#ifndef CANDLE_CHUNK_H
#define CANDLE_CHUNK_H

/*
 * candle_chunk.h - Row-to-column staging unit for a single candle sequence range.
 *
 * A *candle-chunk* owns:
 *   • TRCACHE_NUM_ROW_PAGES lazily-allocated row pages
 *   • one column (SoA) buffer that a copier thread fills
 *   • an atomsnap_gate whose *slot 0 … N-1* hold versioned pointers
 *     to the row pages (NULL ⇒ not yet allocated or already copied)
 *
 * SoA (Structure-of-Arrays) layout bundles each candle field into a
 * separate aligned array, enabling wide SIMD loads / stores.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "concurrent/atomsnap.h"
#include "trcache.h"            /* struct trcache_candle */

#ifndef __cacheline_aligned
#define __cacheline_aligned __attribute__((aligned(64)))
#endif /* __cacheline_aligned */

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
 * @rows:        Fixed AoS storage for TRCACHE_ROWS_PER_PAGE candles
 * @num_filled:  How many rows the producer has written so far
 */
struct candle_row_page {
	struct trcache_candle rows[TRCACHE_ROWS_PER_PAGE];
	_Atomic int num_filled;
} __cacheline_aligned;

/*
 * candle_column_batch - Column (SoA) view of one chunk.
 *
 * Each field pointer is 64-byte aligned for efficient SIMD loads.
 *
 * @field pointers:       SoA arrays – length == TRCACHE_CHUNK_CAP
 * @capacity:             Always TRCACHE_CHUNK_CAP
 * @num_filled:           Rows already copied from row pages
 */
struct candle_column_batch {
	uint64_t *first_timestamp;
	uint64_t *first_trade_id;
	uint32_t *timestamp_interval;
	uint32_t *trade_id_interval;
	double *open;
	double *high;
	double *low;
	double *close;
	double *volume;
	int capacity;
	_Atomic int num_filled;
} __cacheline_aligned;

/*
 * candle_chunk - Owns a sequence window of candles.
 *
 * @seq_begin:            First global sequence number in this chunk (inclusive)
 * @seq_end:              Last  +1  (exclusive)
 * @row_gate:             atomsnap_gate with TRCACHE_NUM_ROW_PAGES slots
 * @column_batch:         Pre-allocated SoA buffer (NULL until first need)
 * @last_row_completed:   Highest sequence no. whose row is finished
 */
struct candle_chunk {
	uint64_t seq_begin;
	uint64_t seq_end;
	struct atomsnap_gate *row_gate;
	struct candle_column_batch *column_batch;
	_Atomic uint64_t last_row_completed;
} __cacheline_aligned;

/**
 * @brief   Translate @seq into a row-page slot index.
 *
 * @return  0‥TRCACHE_NUM_ROW_PAGES-1
 */
static inline int candle_chunk_page_idx(const struct candle_chunk *chunk,
	uint64_t seq)
{
	return (int)((seq - chunk->seq_begin) / TRCACHE_ROWS_PER_PAGE);
}

struct candle_chunk *candle_chunk_create(uint64_t seq_begin);

void candle_chunk_destroy(struct candle_chunk *chunk);

/**
 * @brief   Obtain (and possibly lazily allocate) the mutable row that must
 *          receive the next trade-data update.
 *
 * @param   ck          Candle-chunk pointer.
 * @param   row_out     [out]  Pointer that will receive the trcache_candle row.
 * @param   out_seq     [out]  Absolute sequence number of that row.
 *
 * @return  0 on success, −1 on error (e.g. chunk full / NULL arg).
 *
 * The producer writes trade fields directly into *row_out.  Internal
 * allocation of a fresh page (and its atomsnap gate registration) is done
 * transparently if the row lives on a not-yet-materialised page.
 */
int candle_chunk_get_mutable_row(struct candle_chunk *ck,
	struct trcache_candle **row_out, int *out_seq);

/**
 * @brief   Declare that the row @seq_complete has met the "candle-completed"
 *          condition (OHLCV closed).
 *
 * @param   ck              Candle-chunk pointer.
 * @param   seq_complete    Sequence number of the finished row.
 *
 * Side-effect: updates the chunk’s @last_row_completed.
 */
void candle_chunk_mark_row_complete(struct candle_chunk *ck, int seq_complete);


/**
 * @brief   Acquire the next contiguous range of *completed-but-not-yet-copied*
 *          rows.
 *
 * @param   ck               Candle-chunk pointer.
 * @param   first_seq_ret    [out] First sequence number in the range.
 * @param   nrows_ret        [out] Number of rows in the range (0 ⇒ nothing).
 * @param   row_base_ret     [out] Base address of the first row.
 * @param   page_slot_ret    [out] Slot index of the page’s atomsnap gate
 *                                 (−1 ⇒ the range spans multiple pages or
 *                                 page not yet full).
 *
 * Internal: performs atomsnap_acquire_version() on any page gates that cover
 * the returned rows, ensuring the memory stays valid until the matching
 * commit call.
 */
void candle_chunk_acquire_completed_rows(struct candle_chunk *ck,
	int *first_seq_ret, int *nrows_ret,
	const struct trcache_candle **row_base_ret, int *page_slot_ret);

/**
 * @brief   Commit that the range [first_seq, first_seq + nrows) has been
 *          copied into the column batch.
 *
 * @param   ck             Candle-chunk pointer.
 * @param   first_seq      First sequence of the copied range.
 * @param   nrows          Number of rows copied.
 * @param   page_slot_ret  The same value returned by _acquire_
 *                         (−1 ⇒ partial page copy, keep gate held;
 *                          ≥0 ⇒ full page copied, gate is nulled & released).
 *
 * Handles atomsnap_release_version() and (optionally) gate nullification
 * when the page becomes fully migrated to SoA.
 */
void candle_chunk_commit_copied_rows(struct candle_chunk *ck, int first_seq,
	int nrows, int page_slot_ret);

#endif /* CANDLE_CHUNK_H */

