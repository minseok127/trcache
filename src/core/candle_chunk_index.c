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
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "core/candle_chunk_index.h"
#include "utils/log.h"

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
		errmsg(stderr, "#atomsnap_version allocation failed\n");
		return NULL;
	}

	idx_ver = malloc(sizeof(struct candle_chunk_index_version));

	if (idx_ver == NULL) {
		errmsg(stderr, "#candle_chunk_index_version allocation failed\n");
		free(snap_ver);
		return NULL;
	}

	idx_ver->array = NULL;

#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	idx_ver->array = aligned_alloc(TRCACHE_SIMD_ALIGN,
		newcap * sizeof(struct candle_chunk_index_entry));
	if (idx_ver->array == NULL) {
		errmsg(stderr, "Failure on aligned_alloc() for array\n");
		free(snap_ver);
		free(idx_ver);
		return NULL;
	}
#else
	if (posix_memalign(&idx_ver->array, TRCACHE_SIMD_ALIGN,
			newcap * sizeof(struct candle_chunk_index_entry)) != 0) {
		errmsg(stderr, "Failure on posix_memalign() for array\n");
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
 * @param   init_cap_pow2:           Equals to log2(array_capacity).
 * @param   batch_candle_count_pow2: Equals to log2(batch_candle_count).
 *
 * @return  Pointer to the new index, or NULL on allocation failure.
 */
struct candle_chunk_index *candle_chunk_index_create(int init_cap_pow2,
	int batch_candle_count_pow2)
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
		errmsg(stderr, "#candle_chunk_index allocation failed\n");
		return NULL;
	}

	idx->gate = atomsnap_init_gate(&ctx);
	if (idx->gate == NULL) {
		errmsg(stderr, "Failure on atomsnap_init_gate()\n");
		free(idx);
		return NULL;
	}

	initial_version = atomsnap_make_version(idx->gate, (void*)cap);
	if (initial_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		atomsnap_destroy_gate(idx->gate);
		free(idx);
		return NULL;
	}

	atomsnap_exchange_version(idx->gate, initial_version);
	atomic_store(&idx->head, 0);
	atomic_store(&idx->tail, UINT64_MAX);
	idx->batch_candle_count = 1 << batch_candle_count_pow2;
	idx->batch_candle_count_pow2 = batch_candle_count_pow2;

	return idx;
}

/**
 * @brief   Gracefully destroy the index and all internal arrays.
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
 * @brief   Grow the index's ring buffer.
 *
 * @param   idx:         Pointer of the #candle_chunk_index.
 * @param   cur_idx_ver: Version of the array with no available space.
 * @param   head:        Head at the moment when no slots are available.
 *
 * @return  0 on success, -1 on failure.
 *
 * count1 is the number of entries from the head to the end of the old array,
 * and count2 is the number from the beginning of the old array to (head-1).
 */
static int candle_chunk_index_grow(struct candle_chunk_index *idx,
	struct candle_chunk_index_version *cur_idx_ver, uint64_t head)
{
	uint64_t old_mask = cur_idx_ver->mask;
	uint64_t old_cap = old_mask + 1;
	uint64_t new_cap = old_cap << 1;
	struct atomsnap_version *new_snap
		= atomsnap_make_version(idx->gate, (void *)new_cap);
	struct candle_chunk_index_version *new_idx_ver;
	uint64_t new_mask, count1, count2;

	if (new_snap == NULL ) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		return -1;
	}

	new_mask = new_cap - 1;
	count1 = old_cap - (head & old_mask);
	count2 = old_cap - count1;

	new_idx_ver = (struct candle_chunk_index_version *)new_snap->object;

	memcpy(&new_idx_ver->array[head & new_mask],
		&cur_idx_ver->array[head & old_mask],
		count1 * sizeof(struct candle_chunk_index_entry));

	if (count2 != 0) {
		memcpy(&new_idx_ver->array[old_cap & new_mask], cur_idx_ver->array,
			count2 * sizeof(struct candle_chunk_index_entry));
	}

	atomsnap_exchange_version(idx->gate, new_snap);
	return 0;
}

/**
 * @brief   Append a *newly allocated* chunk to the tail.
 *
 * @param   idx:        Pointer of the #candle_chunk_index.
 * @param   chunk:      Pointer of the newly appended #candle_chunk.
 * @param   seq_first:  First sequence number of the new chunk.
 * @param   ts_first:   First timestamp of the new chunk.
 *
 * @return  0 on success, -1 on failure.
 */
int candle_chunk_index_append(struct candle_chunk_index *idx,
	struct candle_chunk *chunk, uint64_t seq_first, uint64_t ts_first)
{
	uint64_t head = atomic_load_explicit(&idx->head, memory_order_acquire);
	uint64_t tail = atomic_load_explicit(&idx->tail, memory_order_acquire);
	struct atomsnap_version *snap_ver = atomsnap_acquire_version(idx->gate);
	struct candle_chunk_index_version *idx_ver = 
		(struct candle_chunk_index_version *)snap_ver->object;
	uint64_t head_pos = head & idx_ver->mask;
	uint64_t new_tail = tail + 1;
	uint64_t new_tail_pos = new_tail & idx_ver->mask;
	struct candle_chunk_index_entry *entry;

	if (new_tail != 0 && head_pos == new_tail_pos) {
		if (candle_chunk_index_grow(idx, idx_ver, head) == -1) {
			errmsg(stderr, "Failure on candle_chunk_index_grow()\n");
			return -1;
		}

		atomsnap_release_version(snap_ver);
		snap_ver = atomsnap_acquire_version(idx->gate);
		idx_ver = (struct candle_chunk_index_version *)snap_ver->object;
		new_tail_pos = new_tail & idx_ver->mask;
	}

	entry = idx_ver->array + new_tail_pos;
	entry->chunk_ptr = chunk;
	entry->seq_first = seq_first;
	entry->timestamp_first = ts_first;

	atomic_store_explicit(&idx->tail, new_tail, memory_order_release);
	atomsnap_release_version(snap_ver);
	return 0;
}

/**
 * @brief   Remove the oldest chunk if its lifetime has ended.
 *
 * @param   idx: Pointer of the #candle_chunk_index.
 *
 * The caller is responsible for deciding whether the chunk is no longer
 * needed and is not referenced by any thread.
 *
 * @return  Pointer to the popped chunk only for debugging purpose.
 */
#ifdef TRCACHE_DEBUG
struct candle_chunk *candle_chunk_index_pop_head(struct candle_chunk_index *idx)
{
	uint64_t head = atomic_load_explicit(&idx->head, memory_order_acquire);
	struct atomsnap_version *snap_ver = atomsnap_acquire_version(idx->gate);
	struct candle_chunk_index_version *idx_ver = 
		(struct candle_chunk_index_version *)snap_ver->object;
	uint64_t head_pos = head & idx_ver->mask;
	struct candle_chunk_index_entry *entry = idx_ver->array + head_pos;
	struct candle_chunk *chunk = entry->chunk_ptr;

	atomic_store_explicit(&idx->head, head + 1, memory_order_release);
	atomsnap_release_version(snap_ver);
	return chunk;
}
#else  /* !TRCACHE_DEBUG */
void candle_chunk_index_pop_head(struct candle_chunk_index *idx)
{
	atomic_store(&idx->head, idx->head + 1, memory_order_release);
}
#endif /* TRCACHE_DEBUG */

/**
 * @brief   Find the chunk that contains @seq.
 *
 * The caller must ensure that the head does not move.
 *
 * @param   idx:        Pointer of the #candle_chunk_index.
 * @param   target_seq: Target sequence number to search.
 *
 * @return  Pointer to the chunk, or NULL if @seq is outside the index.
 */
struct candle_chunk *candle_chunk_index_find_seq(
	struct candle_chunk_index *idx, uint64_t target_seq)
{
	uint64_t head = atomic_load_explicit(&idx->head, memory_order_acquire);
	uint64_t tail = atomic_load_explicit(&idx->tail, memory_order_acquire);
	struct atomsnap_version *snap_ver = atomsnap_acquire_version(idx->gate);
	struct candle_chunk_index_version *idx_ver;
	uint64_t mask, batch, pow2, first_seq, last_seq, steps, log_idx;
	struct candle_chunk *out;

	if (snap_ver == NULL) {
		errmsg(stderr, "Failure on atomsnap_acquire_version()\n");
		return NULL;
	}

	idx_ver = (struct candle_chunk_index_version *)snap_ver->object;
	mask = idx_ver->mask;
	batch = idx->batch_candle_count;
	pow2 = idx->batch_candle_count_pow2;

	first_seq = idx_ver->array[head & mask].seq_first;
	last_seq = idx_ver->array[tail & mask].seq_first + batch - 1;

	if (target_seq < first_seq || target_seq > last_seq) {
		atomsnap_release_version(snap_ver);
		return NULL;
	}

	steps = (last_seq - target_seq) >> pow2;
	log_idx = tail - steps;
	assert(log_idx >= head);

	out = idx_ver->array[log_idx & mask].chunk_ptr;
	atomsnap_release_version(snap_ver);
	return out;
}

/**
 * @brief   Find the chunk whose [ts_min, ts_max] range contains @ts.
 *
 * The caller must ensure that the head does not move.
 *
 * @param   idx:       Pointer of the #candle_chunk_index.
 * @param   target_ts: Target timestamp to search.
 *
 * @return  Pointer to the chunk, or NULL if @ts is outside the index.
 */
struct candle_chunk *candle_chunk_index_find_ts(
	struct candle_chunk_index *idx, uint64_t target_ts)
{
	uint64_t head = atomic_load_explicit(&idx->head, memory_order_acquire);
	uint64_t tail = atomic_load_explicit(&idx->tail, memory_order_acquire);
	struct atomsnap_version *snap_ver = atomsnap_acquire_version(idx->gate);
	struct candle_chunk_index_version *idx_ver;
	struct candle_chunk *out;
	uint64_t mask, lo = head, hi = tail, mid, ts_mid;

	if (snap_ver == NULL) {
		errmsg(stderr, "Failure on atomsnap_acquire_version()\n");
		return NULL;
	}

	idx_ver = (struct candle_chunk_index_version *)snap_ver->object;
	mask = idx_ver->mask;

	/* Binary search */
	while (lo < hi) {
		mid = lo + ((hi - lo + 1) >> 1);
		ts_mid = idx_ver->array[mid & mask].timestamp_first;

		if (ts_mid <= target_ts) {
			lo = mid;
		} else {
			hi = mid - 1;
		}
	}

	out = idx_ver->array[lo & mask].chunk_ptr;
	atomsnap_release_version(snap_ver);
	return out;
}

