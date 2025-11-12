#ifndef MEMSTAT_H
#define MEMSTAT_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdbool.h>

#include "trcache.h"

/*
 * Padded atomic size_t to prevent false sharing *within an array*.
 * The padding ensures that each element of an array of this struct
 * occupies its own cache line.
 */
struct mem_padded_atomic_size {
	_Atomic size_t value;
	char padding[CACHE_LINE_SIZE - sizeof(_Atomic size_t)];
} ____cacheline_aligned;

/*
 * memory_accounting - Encapsulates global memory tracking and limits.
 *
 * @total_usage:                Current total memory usage. Atomically
 *                              updated *only* by the admin thread.
 * @memory_pressure:            Global flag read by all workers/feed threads.
 *                              Atomically set *only* by the admin thread.
 * @feed_thread_free_list_mem:  Tracks memory in each feed thread's local
 *                              free list. Each element is padded to
 *                              prevent false sharing between threads.
 */
struct memory_accounting {
	____cacheline_aligned
	struct mem_padded_atomic_size total_usage;

	____cacheline_aligned
	_Atomic bool memory_pressure;

	____cacheline_aligned
	struct mem_padded_atomic_size feed_thread_free_list_mem[MAX_NUM_THREADS];

} ____cacheline_aligned;

/**
 * @brief	Atomically add bytes to a cacheline-aligned counter.
 *
 * @param	counter: Pointer to the cacheline-aligned atomic size_t counter.
 * @param	bytes:	 Number of bytes to add.
 */
static inline void mem_add_atomic(_Atomic size_t *counter, size_t bytes)
{
	atomic_fetch_add_explicit(counter, bytes, memory_order_relaxed);
}

/**
 * @brief	Atomically subtract bytes from a cacheline-aligned counter.
 *
 * @param	counter: Pointer to the cacheline-aligned atomic size_t counter.
 * @param	bytes:	 Number of bytes to subtract.
 */
static inline void mem_sub_atomic(_Atomic size_t *counter, size_t bytes)
{
	atomic_fetch_sub_explicit(counter, bytes, memory_order_relaxed);
}

/**
 * @brief	Atomically read the value of a memory counter.
 *
 * @param	counter: Pointer to the atomic size_t counter.
 *
 * @return	Current byte count.
 */
static inline size_t mem_get_atomic(_Atomic size_t *counter)
{
	return atomic_load_explicit(counter, memory_order_relaxed);
}

#endif /* MEMSTAT_H */
