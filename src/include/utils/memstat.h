#ifndef MEMSTAT_H
#define MEMSTAT_H

#include <stdatomic.h>
#include <stddef.h>

#ifndef TRCACHE_CACHELINE_SIZE
#define TRCACHE_CACHELINE_SIZE 64
#endif

typedef enum memstat_category {
	MEMSTAT_TRADE_DATA_BUFFER = 0,
	MEMSTAT_CANDLE_CHUNK_LIST,
	MEMSTAT_CANDLE_CHUNK_INDEX,
	MEMSTAT_SCQ_NODE,
	MEMSTAT_SCHED_MSG,
	MEMSTAT_CATEGORY_NUM
} memstat_category;

struct memstat_entry {
	_Atomic size_t value;
	char padding[TRCACHE_CACHELINE_SIZE - sizeof(size_t)];
};

struct memstat {
	struct memstat_entry category[MEMSTAT_CATEGORY_NUM];
};

/**
 * @brief Global memory counters used by the library.
 */
extern struct memstat g_memstat;

/**
 * @brief Increment the counter for a memory category.
 *
 * @param cat   Category to update.
 * @param bytes Number of bytes to add.
 */
static inline void memstat_add(memstat_category cat, size_t bytes)
{
        atomic_fetch_add_explicit(&g_memstat.category[cat].value,
                bytes, memory_order_relaxed);
}

/**
 * @brief Decrement the counter for a memory category.
 *
 * @param cat   Category to update.
 * @param bytes Number of bytes to subtract.
 */
static inline void memstat_sub(memstat_category cat, size_t bytes)
{
        atomic_fetch_sub_explicit(&g_memstat.category[cat].value,
                bytes, memory_order_relaxed);
}

/**
 * @brief Obtain the current byte count for a category.
 *
 * @param cat Category to query.
 * @return Current number of bytes tracked for @cat.
 */
static inline size_t memstat_get(memstat_category cat)
{
        return atomic_load_explicit(&g_memstat.category[cat].value,
                memory_order_relaxed);
}

/**
 * @brief Print per-category memory statistics to stderr.
 */
void memstat_errmsg_status(void);

#endif /* MEMSTAT_H */
