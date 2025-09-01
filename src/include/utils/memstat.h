#ifndef MEMSTAT_H
#define MEMSTAT_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdbool.h>

#include "trcache.h"

#ifndef TRCACHE_CACHELINE_SIZE
#define TRCACHE_CACHELINE_SIZE 64
#endif

/*
 * memstat_entry - Atomic counter padded to one cacheline.
 */
struct memstat_entry {
	_Atomic size_t value;
	char padding[TRCACHE_CACHELINE_SIZE - sizeof(size_t)];
};

/*
 * memstat - Per-instance memory statistics container.
 *
 * @category:	Byte counters per category.
 */
struct memstat {
	struct memstat_entry category[MEMSTAT_CATEGORY_NUM];
};

/*
 * memory_accounting - Encapsulates the pointers needed for memory accounting.
 *
 * @ms:        Per-category byte counters; zero-initialised on creation.
 * @aux_limit: Maximum number of bytes allowed for auxiliary memory.
 *
 *
 * This structure is embedded in trcache and passed by pointer to
 * modules that need to account memory.
 */
struct memory_accounting {
	struct memstat ms;
	size_t aux_limit;
};

/**
 * @brief	Add bytes to the selected category.
 *
 * @param	ms:	Pointer to memstat structure.
 * @param	cat:	Category index.
 * @param	bytes:	Number of bytes to add.
 */
static inline void memstat_add(struct memstat *ms,
	memstat_category cat, size_t bytes)
{
	atomic_fetch_add_explicit(&ms->category[cat].value,
			bytes, memory_order_relaxed);
}

/**
 * @brief	Subtract bytes from the selected category.
 *
 * @param	ms:	Pointer to memstat structure.
 * @param	cat:	Category index.
 * @param	bytes:	Number of bytes to subtract.
 */
static inline void memstat_sub(struct memstat *ms,
	memstat_category cat, size_t bytes)
{
	atomic_fetch_sub_explicit(&ms->category[cat].value,
			bytes, memory_order_relaxed);
}

/**
 * @brief	Read the byte count for the selected category.
 *
 * @param	ms:	Pointer to memstat structure.
 * @param	cat:	Category index.
 *
 * @return	Current byte count.
 */
static inline size_t memstat_get(struct memstat *ms, memstat_category cat)
{
	return atomic_load_explicit(&ms->category[cat].value,
			memory_order_relaxed);
}

/**
 * @breif   Return the sum of all categories' memory usage.
 *
 * @param   ms: Pointer to a memstat structure.
 *
 * @return  Total bytes allocated across all categories.
 */
static inline size_t memstat_get_total(struct memstat *ms)
{
	size_t total = 0;

	for (int i = 0; i < MEMSTAT_CATEGORY_NUM; i++) {
		total += memstat_get(ms, (enum memstat_category)i);
	}

	return total;
}

/**
 * @brief   Return the sum of all auxiliary memory usage.
 *
 * Sums all memstat categories except MEMSTAT_CANDLE_CHUNK_LIST and
 * MEMSTAT_CANDLE_CHUNK_INDEX.
 *
 * @param   ms: Pointer to a memstat structure.
 *
 * @return  Total bytes allocated across auxiliary categories.
 */
static inline size_t memstat_get_aux_total(struct memstat *ms)
{
	size_t total = 0;

	for (int i = 0; i < MEMSTAT_CATEGORY_NUM; i++) {
		if (i == MEMSTAT_CANDLE_CHUNK_LIST ||
			i == MEMSTAT_CANDLE_CHUNK_INDEX) {
			continue;
		}
		total += memstat_get(ms, (memstat_category)i);
	}

	return total;
}

/**
 * @brief	Print current memory statistics to stderr.
 *
 * @param	ms:	      Pointer to memstat structure.
 * @param   only_aux: Whether skip candle chunk list / index or not.
 */
void memstat_errmsg_status(struct memstat *ms, bool only_aux);

#endif /* MEMSTAT_H */
