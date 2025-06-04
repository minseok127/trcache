#ifndef SYMBOL_TABLE_H
#define SYMBOL_TABLE_H

#include <stddef.h>
#include <pthread.h>

#include "concurrent/atomsnap.h"
#include "pipeline/candle_chunk_list.h"
#include "pipeline/trade_data_buffer.h"
#include "utils/hash_table.h"
#include "sched/sched_pipeline_stats.h"

#include "trcache.h"

/*
 * symbol_entry - Metadata and candle storage for one symbol
 *
 * @candle_chunk_list_ptrs:   Pointer array of candle_chunk_list.
 * @trd_buf:                  Buffer for holding trade data.
 * @pipeline_stats:           Snapshot of pipeline counters and throughput.
 * @symbol_str:               Null-terminated canonical symbol string.
 * @id:                       Symbol ID.
 * @pipeline_stats:           Snapshot of pipeline stage counters.
 */
struct symbol_entry {
	struct candle_chunk_list *candle_chunk_list_ptrs[TRCACHE_NUM_CANDLE_TYPE];
	struct trade_data_buffer *trd_buf;
	struct sched_pipeline_stats pipeline_stats;
	char *symbol_str;
	int id;
	struct sched_pipeline_stats pipeline_stats;
};

/*
 * symbol_table - Structure that manages symbol id and it's entry.
 *
 * @ht_hash_table_mutex:    Protects registration path.
 * @symbol_id_map:          Hash table that maps from string to ID.
 * @symbol_ptr_array_gate:  Gate for snapshot versioning.
 * @num_symbols:            Number of registered symbols.
 * @capacity:               Allocated array capacity.
 *
 * This table holds the symbol entries accessed by both user and system threads.
 * Synchronization for table expansion is handled through atomsnap.
 */
struct symbol_table {
	pthread_mutex_t ht_hash_table_mutex;
	struct ht_hash_table *symbol_id_map;
	struct atomsnap_gate *symbol_ptr_array_gate;
	int num_symbols;
	int capacity;
};

/**
 * @brief   Create a new symbol_table.
 *
 * @param   initial_capacity: Initial bucket/array size (power of two).
 *
 * @return  Pointer to a newly allocated symbol_table, or NULL on error.
 *
 * @thread-safety Single-threaded: must be called before any concurrent access.
 */
struct symbol_table *symbol_table_init(int initial_capacity);

/**
 * @brief   Destroy a symbol_table and free all resources.
 *
 * @param   symbol_table:	Pointer returned by init_symbol_table().
 *
 * @thread-safety Single-threaded: ensure no other threads are using the table.
 */
void symbol_table_destroy(struct symbol_table *symbol_table);

/**
 * @brief   Lookup a symbol entry by ID.
 *
 * Lock-free, reader-safe via atomsnap.
 *
 * @param   table:     Pointer to symbol_table.
 * @param   symbol_id: Symbol ID to lookup.
 *
 * @return  Pointer to #symbol_entry, or NULL if out of range.
 *
 * @thread-safety Safe for concurrent readers.
 */
struct symbol_entry *symbol_table_lookup_entry(
	struct symbol_table *table, int symbol_id);

/**
 * @brief   Lookup symbol ID by its string name.
 *
 * Performs a mutex-protected hash lookup.
 *
 * @param   table:      Pointer to symbol_table.
 * @param   symbol_str: NULL-terminated symbol string.
 *
 * @return  Symbol ID >=0 on success, or -1 if not found.
 *
 * @thread-safety Safe for concurrent callers; protected by internal mutex.
 */
int symbol_table_lookup_symbol_id(
	struct symbol_table *table, const char *symbol_str);

/**
 * @brief   Register a new symbol or return existing ID.
 *
 * Inserts the string into the internal hash map and expands 
 * symbol table via copy-on-write if needed.
 *
 * @param   table:      Pointer to symbol_table.
 * @param   symbol_str: NULL-terminated symbol string.
 *
 * @return  Assigned symbol ID >=0, or -1 on error.
 *
 * @thread-safety Safe for concurrent callers; registration path is mutex-protected.
 */
int symbol_table_register(struct trcache *tc, struct symbol_table *table,
	const char *symbol_str);

#endif /* SYMBOL_TABLE_H */
