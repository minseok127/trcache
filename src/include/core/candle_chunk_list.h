#ifndef CANDLE_CHUNK_LIST_H
#define CANDLE_CHUNK_LIST_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

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
 * @rows:        Fixed AoS storage for TRCACHE_ROWS_PER_PAGE candles
 */
struct candle_row_page {
	struct trcache_candle rows[TRCACHE_ROWS_PER_PAGE];
};

/*
 * candle_chunk - Owns a sequence window of candles.
 *
 * @seq_begin:            First global sequence number in this chunk (inclusive)
 * @seq_end:              Last  +1  (exclusive)
 * @row_gate:             atomsnap_gate with TRCACHE_NUM_ROW_PAGES slots
 * @column_batch:         SoA buffer (NULL until first need)
 * @last_row_completed:   Highest sequence no. whose row is finished
 * @last_row_copied:      Highest seq already copied to COLUMN
 */
struct candle_chunk {
	uint64_t seq_begin;
	uint64_t seq_end;
	struct atomsnap_gate *row_gate;
	struct trcache_candle_batch *column_batch;
	_Atomic uint64_t last_row_completed;
	_Atomic uint64_t last_row_copied;
};

/*
 * candle_chunk_mutate_handle - Writer handle for a single mutable row.
 *
 * @page_version:   Atomsnap version held
 * @row_ptr:        Pointer to mutable row
 * @seq:            Sequence number of the row
 * @num_mutated:    How many mutation applied
 */
struct candle_chunk_mutate_handle {
	struct atomsnap_version *page_version;
	struct trcache_candle *row_ptr;
	uint64_t seq;
	int num_mutated;
};

/*
 * candle_chunk_copy_handle - Converter handle for row -> column batch copy.
 *
 * @page_version: Atomsnap version held
 * @row_base:     Pointer to first row
 * @start_seq:    First sequence copied
 * @row_count:    Number of rows
 * @num_copied:   How many rows are copied
 *
 * This is filled by candle_chunk_acquire_completed_rows() and must be passed
 * unchanged to candle_chunk_commit_copied_rows().
 */
struct candle_chunk_copy_handle {
	struct atomsnap_version *page_version;
	const struct trcache_candle *row_base;
	uint64_t start_seq;
	int row_count;
	int num_copied;
};

/*
 * candle_chunk_flush_handle - Flush‑worker handle.
 *
 * @chunk: chunkt to flush
 */
struct candle_chunk_flush_handle {
	struct candle_chunk *chunk;
};

struct candle_chunk_list {
	struct candle_chunk_mutate_handle mutate_handle;
	struct candle_chunk_copy_handle copy_handle;
	struct candle_chunk_flush_handle flush_handle;
};

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

/**
 * @brief Allocate and initialize a candle chunk.
 *
 * @param seq_begin: first sequence number contained in this chunk (inclusive).
 *
 * The function sets up an atomsnap gate with one slot per row page.
 * Each slot lazily materializes its row page through row_page_version_alloc().
 *
 * @return Pointer to the new chunk, or NULL on failure.
 */
struct candle_chunk *candle_chunk_create(uint64_t seq_begin);

/**
 * @brief Release all resources of a candle chunk.
 *
 * @param chunk: Candle-chunk pointer.
 */
void candle_chunk_destroy(struct candle_chunk *chunk);

/**
 * @brief   Obtain (and possibly lazily allocate) the mutable row that must
 *          receive the next trade-data update.
 *
 * @param   chunk:         Candle-chunk pointer.
 * @param   handle [out]:  Output handle filled with mutable candle.
 *
 * Internal allocation of a fresh page (and its atomsnap gate registration) is
 * done transparently if the row lives on a not-yet-materialised page.
 */
void candle_chunk_get_mutable_row(struct candle_chunk *chunk,
	struct candle_chunk_mutate_handle *handle);

/**
 * @brief   Declare that the row @seq_complete has met the "candle-completed"
 *          condition (OHLCV closed).
 *
 * @param   chunk:        Candle-chunk pointer.
 * @param   handle:       Handle filled with mutated candle.
 *
 * Side-effect: updates the chunk’s @last_row_completed.
 */
void candle_chunk_commit_immutable_row(struct candle_chunk *chunk,
	const struct candle_chunk_mutate_handle *handle);

/**
 * @brief        Get the next contiguous range of *completed-but-uncopied*
 *               rows that still reside in row pages.
 *
 * The range is **always confined to a single row-page**; caller repeats
 * until this function returns `false`.
 *
 * Internally the routine acquires the page’s atomsnap version so the
 * memory stays valid until the matching commit.
 *
 * @param chunk:         Candle-chunk pointer.
 * @param handle [out]:  Output handle filled with completed candles.
 *
 * @return 'true'  if there are remained rows in a next page.
 *         'false' if nothing is left to copy.
 */
bool candle_chunk_acquire_completed_rows(struct candle_chunk *chunk,
	struct candle_chunk_copy_handle *handle);

/**
 * @brief        Commit that every row up-to @p last_seq_copied (inclusive)
 *               has been transferred into the column batch buffer.
 *
 * The function:
 *   - advances `ck->last_row_copied`  
 *   - releases (and, if fully migrated, NULL-ifies) the row-page’s
 *     atomsnap version that was held by the preceding *acquire*.
 *
 * @param chunk:  Candle-chunk pointer.
 * @param handle: Handle filled with copied candles. 
 */
void candle_chunk_commit_copied_rows(struct candle_chunk *ck,
	const struct candle_chunk_copy_handle *handle);


#endif /* CANDLE_CHUNK_LIST_H */

