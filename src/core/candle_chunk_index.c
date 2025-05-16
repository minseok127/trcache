/**
 * @file   candle_chunk_index.c
 * @brief  Public interface for the lock-free candle-chunk index.
 *
 * Only **one writer thread** and **one deleter thread** are allowed to
 * mutate the index concurrently; any number of reader threads may call
 * the lookup helpers.
 *
 * Memory-ordering rules:
 *   - Writer / deleter store @tail / @head with
 *     'memory_order_release'.
 *   - Readers load them with 'memory_order_acquire', *before* acquiring
 *     the atomsnap version (see struct comment in the implementation).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "core/candle_chunk_index.h"

/**
 * @brief Allocate and initialise an empty index.
 *
 * @param init_cap_pow2: Initial capacity expressed as log2(capacity).
 *                       Must be ≥ 3 (i.e. capacity ≥ 8).
 *
 * @return Pointer to the new index, or NULL on allocation failure.
 */
struct candle_chunk_index *candle_chunk_index_create(unsigned init_cap_pow2)
{

}

/**
 * @brief Gracefully destroy the index and all internal arrays.
 *
 * The call waits for the atomsnap grace-period so that no reader holds
 * a reference to any retired array when memory is freed.
 */
void candle_chunk_index_destroy(struct candle_chunk_index *idx)
{

}

/**
 * @brief Append a *newly allocated* chunk to the tail.
 *
 * The function publishes a provisional entry whose @seq_last,
 * @ts_max fields are UINT64_MAX. They must be filled in later
 * with candle_chunk_index_finalize().
 *
 * @param seq_first:  First sequence number of the new chunk.
 * @param ts_min:     First timestamp of the new chunk.
 *
 * @return 0 on success, -1 on failure.
 */
int candle_chunk_index_append(struct candle_chunk_index *idx,
	struct candle_chunk *chunk, uint64_t seq_first, uint64_t ts_min)
{

}

/**
 * @brief Fill in the final range of a chunk once it becomes immutable.
 *
 * Must be called by the writer thread **exactly once** per chunk after
 * the last candle has been closed.
 *
 * @param chunk:      Pointer identical to the one passed to *append*.
 * @param seq_last:   Last sequence number stored in the chunk.
 * @param ts_max:     Maximum timestamp in the chunk.
 *
 * @return 0 on success, -1 if the chunk is not found.
 */
int candle_chunk_index_finalize(struct candle_chunk_index *idx,
	struct candle_chunk *chunk, uint64_t seq_last, uint64_t ts_max)
{

}

/**
 * @brief Remove the oldest chunk if its lifetime has ended.
 *
 * The caller is responsible for deciding whether the chunk is no longer
 * needed (e.g. persisted to disk). On success the function advances
 * @head and returns the retired chunk pointer.
 *
 * @return Pointer to the removed chunk, or NULL if the index is empty.
 */
struct candle_chunk *candle_chunk_index_pop_head(
	struct candle_chunk_index *idx)
{

}


/**
 * @brief Locate the chunk that contains @seq.
 *
 * @return Pointer to the chunk, or NULL if @p seq is outside the index.
 */
struct candle_chunk *candle_chunk_index_find_seq(
	struct candle_chunk_index *idx, uint64_t seq)
{

}

/**
 * @brief Locate the chunk whose [ts_min, ts_max] range contains @ts.
 *
 * @return Pointer to the chunk, or NULL if no such chunk exists.
 */
struct candle_chunk *candle_chunk_index_find_ts(
	struct candle_chunk_index *idx, uint64_t ts)
{

}

