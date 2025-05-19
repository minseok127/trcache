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
 * @brief   Allocate atomsnap version and index version.
 *
 * @param   newcap: New capacity of the index's array (must be a power of 2).
 *
 * @return  Pointer to the atomsnap version, NULL on failure.
 */
static struct atomsnap_version *candle_chunk_index_version_alloc(void *cap)
{
	struct atomsnap_version *snap_ver = malloc(sizeof(struct atomsnap_version));
	struct candle_chunk_index_version *idx_ver = NULL;
	uint64_t newcap = (uint64_t)cap;

	if (snap_ver == NULL) {
		fprintf(stderr, "candle_chunk_index_version_alloc: snap failed\n");
		return NULL;
	}

	idx_ver = malloc(sizeof(struct candle_chunk_index_version));

	if (idx_ver == NULL) {
		fprintf(stderr, "candle_chunk_index_version_alloc: idx failed\n");
		free(snap_ver);
		return NULL;
	}

	idx_ver->array = NULL;

#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	idx_ver->array = aligned_alloc(TRCACHE_SIMD_ALIGN,
		newcap * sizeof(struct candle_chunk_index_entry));
	if (idx_ver->array == NULL) {
		fprintf(stderr, "candle_chunk_index_version_alloc: array failed\n");
		free(snap_ver);
		free(idx_ver);
		return NULL;
	}
#else
	if (posix_memalign(&idx_ver->array, TRCACHE_SIMD_ALIGN,
			newcap * sizeof(struct candle_chunk_index_entry)) != 0) {
		fprintf(stderr, "candle_chunk_index_version_alloc: array failed\n");
		free(snap_ver);
		free(idx_ver);
		return NULL;
	}
#endif

	snap_ver->object = idx_ver;
	idx_ver->mask = newcap - 1;

	return snap_ver;
}

/**
 * @brief   Free the atomsnap version and it's index version.
 *
 * @param   snap_ver: An atomsnap version to free.
 */
static void candle_chunk_index_version_free(struct atomsnap_version *snap_ver)
{
	struct candle_chunk_index_version *idx_ver
		= (struct candle_chunk_index_version *)snap_ver->object;

	free(idx_ver->array);
	free(idx_ver);
	free(snap_ver);
}

/**
 * @brief   Allocate and initialise an empty index.
 *
 * @param   init_cap_pow2: Initial capacity expressed as log2(capacity).
 *
 * @return  Pointer to the new index, or NULL on allocation failure.
 */
struct candle_chunk_index *candle_chunk_index_create(unsigned init_cap_pow2)
{
	uint64_t cap = 1ULL << init_cap_pow2;
	struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = candle_chunk_index_version_alloc,
		.atomsnap_free_impl = candle_chunk_index_version_free,
		.num_extra_control_blocks = 0
	};
	struct atomsnap_version *initial_version = NULL;
	struct candle_chunk_index *idx = malloc(sizeof(struct candle_chunk_index));

	if (idx == NULL) {
		fprintf(stderr, "candle_chunk_index_create: idx alloc failed\n");
		return NULL;
	}

	idx->gate = atomsnap_init_gate(&ctx);
	if (idx->gate == NULL) {
		fprintf(stderr, "candle_chunk_index_create: gate init failed\n");
		free(idx);
		return NULL;
	}

	initial_version = atomsnap_make_version((void*)cap);
	if (initial_version == NULL) {
		fprintf(stderr, "candle_chunk_index_create: initial version failed\n");
		atomsnap_destroy_gate(idx->gate);
		free(idx);
		return NULL;
	}

	atomsnap_exchange_version(idx->gate, initial_version);
	atomic_store(&idx->head, UINT64_MAX);
	atomic_store(&idx->tail, UINT64_MAX);

	return idx;
}

/**
 * @brief   Gracefully destroy the index and all internal arrays.
 *
 * The call waits for the atomsnap grace-period so that no reader holds
 * a reference to any retired array when memory is freed.
 *
 * @param   idx: Pointer from the candle_chunk_index_create().
 */
void candle_chunk_index_destroy(struct candle_chunk_index *idx)
{
	if (idx == NULL) {
		return;
	}

	atomsnap_destroy_gate(idx->gate);
	free(idx);
}

/**
 * @brief   Append a *newly allocated* chunk to the tail.
 *
 * The function publishes a provisional entry whose @seq_last,
 * @ts_max fields are UINT64_MAX. They must be filled in later
 * with candle_chunk_index_finalize().
 *
 * @param   idx:        Pointer of the #candle_chunk_index.
 * @param   chunk:      Pointer of the newly appended #candle_chunk.
 * @param   seq_first:  First sequence number of the new chunk.
 * @param   ts_min:     First timestamp of the new chunk.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_index_append(struct candle_chunk_index *idx,
	struct candle_chunk *chunk, uint64_t seq_first, uint64_t ts_min)
{
	uint64_t head = atomic_load_explicit(&idx->head, memory_order_acquire);
	uint64_t tail = atomic_load_explicit(&idx->tail, memory_order_acquire);
	uint64_t new_tail = tail + 1;

	if (head == new_tail) {
		
	}
}

/**
 * @brief   Fill in the final range of a chunk once it becomes immutable.
 *
 * Must be called by the writer thread **exactly once** per chunk after
 * the last candle has been closed.
 *
 * @param   chunk:      Pointer identical to the one passed to *append*.
 * @param   seq_last:   Last sequence number stored in the chunk.
 * @param   ts_max:     Maximum timestamp in the chunk.
 *
 * @return  0 on success, -1 if the chunk is not found.
 */
int candle_chunk_index_finalize(struct candle_chunk_index *idx,
	struct candle_chunk *chunk, uint64_t seq_last, uint64_t ts_max)
{

}

/**
 * @brief   Remove the oldest chunk if its lifetime has ended.
 *
 * The caller is responsible for deciding whether the chunk is no longer
 * needed (e.g. persisted to disk). On success the function advances
 * @head and returns the retired chunk pointer.
 *
 * @return  Pointer to the removed chunk, or NULL if the index is empty.
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

