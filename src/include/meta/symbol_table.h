#ifndef SYMBOL_TABLE_H
#define SYMBOL_TABLE_H

#include <stddef.h>
#include <pthread.h>
#include <stdatomic.h>

#include "pipeline/candle_chunk_list.h"
#include "pipeline/trade_data_buffer.h"
#include "utils/hash_table.h"

#include "trcache.h"

/*
 * symbol_entry - Metadata and candle storage for one symbol.
 *
 * @candle_chunk_list_ptrs:   Pointer array of candle_chunk_list.
 * @trd_buf:                  Buffer for holding trade data.
 * @symbol_str:               Null-terminated canonical symbol string.
 * @id:                       Symbol ID.
 */
struct symbol_entry {
	struct candle_chunk_list *candle_chunk_list_ptrs[MAX_CANDLE_TYPES];
	struct trade_data_buffer *trd_buf;
	char *symbol_str;
	int id;
};

/*
 * in_memory_owner - Global ownership flags for In-Memory stages.
 *
 * Stored in (candle_type, symbol_id) major order:
 * (T0,S0), (T0,S1)... (T0,Sn), (T1,S0), (T1,S1)...
 *
 * @apply_taken:   Ownership flag for the APPLY stage (-1 = free, 1 = taken).
 * @convert_taken: Ownership flag for the CONVERT stage (-1 = free, 1 = taken).
 */
struct in_memory_owner {
	_Atomic(int) apply_taken;
	_Atomic(int) convert_taken;
};

/*
 * symbol_table - Structure that manages symbol id and it's entry.
 *
 * @ht_hash_table_mutex:       Protects registration path.
 * @symbol_id_map:             Hash table that maps from string to ID.
 * @symbol_entries:            Pre-allocated array of all symbol entries.
 * @num_symbols:               Number of registered symbols.
 * @capacity:                  Allocated array capacity.
 * @in_memory_ownership_flags: Array of ownership flags for Apply/Convert.
 * @flush_ownership_flags:     Array of ownership flags for Flush.
 *
 * The data arrays pointed to by the ownership flags will be
 * allocated on a cache line boundary in symbol_table_init().
 */
struct symbol_table {
	pthread_mutex_t ht_hash_table_mutex;
	struct ht_hash_table *symbol_id_map;
	struct symbol_entry *symbol_entries;
	_Atomic(int) num_symbols;
	int capacity;
	struct in_memory_owner *in_memory_ownership_flags;
	_Atomic(int) *flush_ownership_flags;
};

/**
 * @brief   Create a new symbol_table.
 *
 * @param   max_capacity:       The maximum number of symbols.
 * @param   num_candle_configs: The number of candle types.
 *
 * @return  Pointer to a newly allocated symbol_table, or NULL on error.
 *
 */
struct symbol_table *symbol_table_init(int max_capacity,
	int num_candle_configs);

/**
 * @brief   Destroy a symbol_table and free all resources.
 *
 * @param   symbol_table:	Pointer returned by init_symbol_table().
 */
void symbol_table_destroy(struct symbol_table *symbol_table);

/**
 * @brief   Lookup a symbol by ID.
 *
 * @param   table:     Pointer to symbol_table.
 * @param   symbol_id: Symbol ID to lookup.
 *
 * @return  Pointer to #symbol_entry, or NULL if out of range.
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
