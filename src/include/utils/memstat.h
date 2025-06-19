#ifndef MEMSTAT_H
#define MEMSTAT_H

#include <stdatomic.h>
#include <stddef.h>

#ifndef TRCACHE_CACHELINE_SIZE
#define TRCACHE_CACHELINE_SIZE 64
#endif

/*
 * memstat_category - Memory usage categories tracked by memstat.
 */
typedef enum memstat_category {
	MEMSTAT_TRADE_DATA_BUFFER = 0,
	MEMSTAT_CANDLE_CHUNK_LIST,
	MEMSTAT_CANDLE_CHUNK_INDEX,
	MEMSTAT_SCQ_NODE,
	MEMSTAT_SCHED_MSG,
	MEMSTAT_CATEGORY_NUM
} memstat_category;

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
 * @brief	Print current memory statistics to stderr.
 *
 * @param	ms:	Pointer to memstat structure.
 */
void memstat_errmsg_status(struct memstat *ms);

#endif /* MEMSTAT_H */
