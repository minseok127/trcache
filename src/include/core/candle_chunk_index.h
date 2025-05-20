#ifndef CANDLE_CHUNK_INDEX_H
#define CANDLE_CHUNK_INDEX_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "core/candle_chunk.h"

/*
 * candle_chunk_index_entry - Metadata that maps a range of candles to a chunk.
 
 * @chunk_ptr:       Pointer to the physical chunk that stores the candles.
 * @seq_first:       Sequence number (inclusive) of the first candle.
 * @timestamp_first: Timestamp (inclusive) of the first candle.
 */
struct candle_chunk_index_entry {
	struct candle_chunk *chunk_ptr;
	uint64_t seq_first;
	uint64_t timestamp_first;
};

/**
 * candle_chunk_index_version - Version of array and it's mask.
 *
 * @array: Pointer to the entry array (capacity is implicit in @mask). 
 * @mask:  Power-of-two capacity mask (cap − 1).
 *
 * Physical slot index = logical_idx & mask.
 */
struct candle_chunk_index_version {
	struct candle_chunk_index_entry *array;
	uint64_t mask;
};

/*
 * candle_chunk_index - Lock-free ring-buffer that maps candles to chunks.
 *
 * @gate:  Atomsnap gate that publishes {entry_array*, mask}.
 * @head: Logical position of the oldest live entry.
 * @tail: Logical position one past the newest live entry.
 *
 * The index is a *single-producer / single-consumer* ring with many concurrent
 * readers. A writer (producer) appends one #candle_chunk_index_entry per newly
 * allocated chunk; a deleter (consumer) increments @head once an old chunk
 * becomes eligible for reclamation. Readers acquire a snapshot of the
 * <array,mask> pair via @atomsnap_gate so they can dereference entries without
 * additional synchronisation.
 *
 * Versioning strategy
 * -------------------
 * The *entry array* itself is **immutable** once published.  
 * When the ring becomes full, the writer allocates a new array
 * (capacity × 2), copies live entries in two blocks
 *
 *   1) 'head ... old_cap-1'  ->  same index  
 *   2) '0 ... head-1'        ->  'old_cap ... old_cap+head-1'
 *
 * and then publishes the new <pointer,mask> as a fresh atomsnap version.
 * The old array is retired only after the atomsnap grace period ends,
 * guaranteeing reader safety.
 *
 * Order matters for readers.
 * --------------------------
 * If a reader were to acquire the atomsnap version *first* and then read
 * @tail, it might obtain an *old* array (small 'mask') but a *new*
 * tail value already incremented by the writer after a grow(). The slot
 * calculation 'tail & mask' could then dereference invalid memory.
 *
 * Therefore every reader must:
 *   1. 'head = atomic_load_acquire(&head);'
 *      'tail = atomic_load_acquire(&tail);'
 *   2. 'ver  = atomsnap_acquire_version(&gate);'
 *      'mask = ver->mask;'
 *
 * The writer always publishes the new <array,mask> *before* it increments
 * @tail for the first entry that resides in the new array.
 */
struct candle_chunk_index {
	struct atomsnap_gate *gate;
	_Atomic uint64_t head;
	_Atomic uint64_t tail;
};

/**
 * @brief   Allocate and initialise an empty index.
 *
 * @param   init_cap_pow2: Initial capacity expressed as log2(capacity).
 *
 * @return  Pointer to the new index, or NULL on allocation failure.
 */
struct candle_chunk_index *candle_chunk_index_create(int init_cap_pow2);

/**
 * @brief   Gracefully destroy the index and all internal arrays.
 *
 * @param   idx: Pointer from the candle_chunk_index_create().
 */
void candle_chunk_index_destroy(struct candle_chunk_index *idx);

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
	struct candle_chunk *chunk, uint64_t seq_first, uint64_t ts_first);

/**
 * @brief   Remove the oldest chunk if its lifetime has ended.
 *
 * The caller is responsible for deciding whether the chunk is no longer
 * needed and is not referenced by any thread. On success the function advances
 * @head and returns the retired chunk pointer.
 *
 * @return  Pointer to the removed chunk, or NULL if the index is empty.
 */
struct candle_chunk *candle_chunk_index_pop_head(
	struct candle_chunk_index *idx);


/**
 * @brief   Locate the chunk that contains @seq.
 *
 * @return  Pointer to the chunk, or NULL if @p seq is outside the index.
 */
struct candle_chunk *candle_chunk_index_find_seq(
	struct candle_chunk_index *idx, uint64_t seq);

/**
 * @brief   Locate the chunk whose [ts_min, ts_max] range contains @ts.
 *
 * @return  Pointer to the chunk, or NULL if no such chunk exists.
 */
struct candle_chunk *candle_chunk_index_find_ts(
	struct candle_chunk_index *idx, uint64_t ts);

#endif /* CANDLE_CHUNK_INDEX_H */
