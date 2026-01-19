/**
 * @file    core/symbol_table.c
 * @brief   Implements a thread-safe symbol table with lock-free reads by
 *          atomsnap and mutex-protected copy-on-write updates for writes.
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdatomic.h>

#include "meta/symbol_table.h"
#include "meta/trcache_internal.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/hash_table_callbacks.h"
#include "utils/log.h"

#include "trcache.h"

#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))

/**
 * @brief   Create a new symbol_table.
 *
 * @param   max_capacity:       The maximum number of symbols.
 * @param   num_candle_configs: The number of candle types.
 *
 * @return  Pointer to a newly allocated symbol_table, or NULL on error.
 *
 */
struct symbol_table *symbol_table_init(int max_capacity, int num_candle_configs)
{
	struct symbol_table *table = calloc(1, sizeof(struct symbol_table));
	size_t num_tasks, in_mem_size, flush_size;

	if (table == NULL) {
		errmsg(stderr, "#symbol_table allocation failed\n");
		return NULL;
	}

	pthread_mutex_init(&table->ht_hash_table_mutex, NULL);

	/* Create ID map with string callbacks */
	table->symbol_id_map = ht_create(
		8192,					/* initial capacity */
		0xDEADBEEFULL,			/* seed */
		murmur_hash,			/* hash function */
		compare_symbol_str,		/* cmp_func */
		duplicate_symbol_str,	/* dup_func */
		free_symbol_str			/* free_func */
	);

	if (table->symbol_id_map == NULL) {
		errmsg(stderr, "Failure on ht_create()\n");
		pthread_mutex_destroy(&table->ht_hash_table_mutex);
		free(table);
		return NULL;
	}

	/* Pre-allocate the fixed-size symbol entry array */
	table->symbol_entries = calloc(max_capacity, sizeof(struct symbol_entry));
	if (table->symbol_entries == NULL) {
		errmsg(stderr, "Allocation of symbol_entries array failed\n");
		ht_destroy(table->symbol_id_map);
		pthread_mutex_destroy(&table->ht_hash_table_mutex);
		free(table);
		return NULL;
	}

	/*
	 * Allocate and initialize the new global ownership flag arrays.
	 * All flags are initialized to -1 (free).
	 */
	num_tasks = (size_t)num_candle_configs * (size_t)max_capacity;
	in_mem_size = num_tasks * sizeof(struct in_memory_owner);
	flush_size = num_tasks * sizeof(_Atomic(int));

	table->in_memory_ownership_flags = aligned_alloc(
		CACHE_LINE_SIZE, ALIGN_UP(in_mem_size, CACHE_LINE_SIZE));
	if (table->in_memory_ownership_flags == NULL) {
		errmsg(stderr, "in_memory_ownership_flags allocation failed\n");
		free(table->symbol_entries);
		ht_destroy(table->symbol_id_map);
		pthread_mutex_destroy(&table->ht_hash_table_mutex);
		free(table);
		return NULL;
	}
	/* Initialize all flags to -1 (free) */
	memset(table->in_memory_ownership_flags, -1, in_mem_size);

	table->flush_ownership_flags = aligned_alloc(
		CACHE_LINE_SIZE, ALIGN_UP(flush_size, CACHE_LINE_SIZE));
	if (table->flush_ownership_flags == NULL) {
		errmsg(stderr, "flush_ownership_flags allocation failed\n");
		free(table->in_memory_ownership_flags);
		free(table->symbol_entries);
		ht_destroy(table->symbol_id_map);
		pthread_mutex_destroy(&table->ht_hash_table_mutex);
		free(table);
		return NULL;
	}
	/* Initialize all flags to -1 (free) */
	memset(table->flush_ownership_flags, -1, flush_size);

	table->capacity = max_capacity;
	table->num_symbols = 0;

	return table;
}

/**
 * @brief   Destroy a symbol_table and free all resources.
 *
 * @param   symbol_table:	Pointer returned by init_symbol_table().
 */
void symbol_table_destroy(struct symbol_table *table)
{
	struct symbol_entry *entry = NULL;

	if (table == NULL) {
		return;
	}

	ht_destroy(table->symbol_id_map);

	pthread_mutex_destroy(&table->ht_hash_table_mutex);

	for (int i = 0; i < table->num_symbols; i++) {
		entry = &table->symbol_entries[i];

		for (int j = 0; j < MAX_CANDLE_TYPES; j++) {
			if (entry->candle_chunk_list_ptrs[j] != NULL) {
				candle_chunk_list_finalize(entry->candle_chunk_list_ptrs[j]);
				destroy_candle_chunk_list(entry->candle_chunk_list_ptrs[j]);
			}
		}

		trade_data_buffer_destroy(entry->trd_buf);
		free(entry->symbol_str);
	}

	free(table->in_memory_ownership_flags);
	free(table->flush_ownership_flags);

	free(table->symbol_entries);
	free(table);
}

/**
 * @brief   Lookup a symbol by ID.
 *
 * @param   table:     Pointer to symbol_table.
 * @param   symbol_id: Symbol ID to lookup.
 *
 * @return  Pointer to #symbol_entry, or NULL if out of range.
 */
struct symbol_entry *symbol_table_lookup_entry(
	struct symbol_table *table, int symbol_id)
{
	struct symbol_entry *result = NULL;

	if (symbol_id >= 0 && symbol_id < table->num_symbols) {
		result = &table->symbol_entries[symbol_id];
	}

	return result;
}

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
int symbol_table_lookup_symbol_id(struct symbol_table *table,
	const char *symbol_str)
{
	bool found = false;
	int symbol_id = -1;

	pthread_mutex_lock(&table->ht_hash_table_mutex);

	symbol_id = (int)(uintptr_t) ht_find(table->symbol_id_map, symbol_str,
		strlen(symbol_str) + 1, /* string + NULL */
		&found);

	pthread_mutex_unlock(&table->ht_hash_table_mutex);

	return (found ? symbol_id : -1);
}

/**
 * @brief   Initialize a new symbol entry.
 *
 * @param   tc:          Pointer to the #trcache.
 * @param   entry:       Pointer to the pre-allocated symbol_entry slot.
 * @param   id:          Symbol ID to assign.
 * @param   symbol_str:  Symbol string to assign.
 *
 * @return  0 on success, -1 on failure.
 */
static int init_symbol_entry(struct trcache *tc,
	struct symbol_entry *entry, int id, const char *symbol_str)
{
	struct candle_chunk_list_init_ctx ctx = { 0, };
	struct candle_chunk_list *candle_chunk_list_ptr;

	entry->symbol_str = duplicate_symbol_str(symbol_str,
		strlen(symbol_str) + 1);

	if (entry->symbol_str == NULL) {
		errmsg(stderr, "Failure on duplicate_symbol_str()\n");
		return -1;
	}

	entry->trd_buf = trade_data_buffer_init(tc, id);

	if (entry->trd_buf == NULL) {
		errmsg(stderr, "Allocation of trade_data_buffer is failed\n");
		free(entry->symbol_str);
		entry->symbol_str = NULL;
		return -1;
	}

	for (int i = 0; i < tc->num_candle_configs; i++) {
		ctx.trc = tc;
		ctx.candle_idx = i;
		ctx.symbol_id = id;

		candle_chunk_list_ptr = create_candle_chunk_list(&ctx);
		if (candle_chunk_list_ptr == NULL) {
			errmsg(stderr, "Candle chunk list allocation is failed\n");

			for (int j = 0; j < i; j++) {
				destroy_candle_chunk_list(entry->candle_chunk_list_ptrs[j]);
			}

			trade_data_buffer_destroy(entry->trd_buf);
			free(entry->symbol_str);
			entry->symbol_str = NULL;
			entry->trd_buf = NULL;
			return -1;
		}

		entry->candle_chunk_list_ptrs[i] = candle_chunk_list_ptr;
	}

	entry->id = id;

	return 0;
}

/**
 * @brief   Register a new symbol or return existing ID.
 *
 * Inserts the string into the internal hash map and expands
 * symbol table via copy-on-write if needed.
 *
 * @param   tc:         Pointer to the #trcache.
 * @param   table:      Pointer to symbol_table.
 * @param   symbol_str: NULL-terminated symbol string.
 *
 * @return  Assigned symbol ID >=0, or -1 on error.
 *
 * @thread-safety Safe for concurrent callers; registration path is mutex-protected.
 */
int symbol_table_register(struct trcache *tc, struct symbol_table *table,
	const char *symbol_str)
{
	struct symbol_entry *entry = NULL;
	bool found = false;
	void *value;
	int id;

	pthread_mutex_lock(&table->ht_hash_table_mutex);

	/* Check if symbol is already registered */
	value = ht_find(table->symbol_id_map, symbol_str,
			strlen(symbol_str) + 1, &found);
	if (found) {
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return (int)(uintptr_t)value;
	}

	id = table->num_symbols;

	/* Check if table is full */
	if (id >= table->capacity) {
		errmsg(stderr, "Symbol table is full. Max capacity: %d\n",
			table->capacity);
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	/* Insert into hash map first */
	if (ht_insert(table->symbol_id_map, symbol_str,
			strlen(symbol_str) + 1, /* string + NULL */
			(void *)(uintptr_t)id) < 0) {
		errmsg(stderr, "Failure on ht_insert()\n");
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	/* Get the pre-allocated slot and initialize it */
	entry = &table->symbol_entries[id];
	if (init_symbol_entry(tc, entry, id, symbol_str) != 0) {
		errmsg(stderr, "Failure on init_symbol_entry() for id %d\n", id);
		/* Rollback ht_insert */
		ht_remove(table->symbol_id_map, symbol_str, strlen(symbol_str) + 1);
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	table->num_symbols++;

	pthread_mutex_unlock(&table->ht_hash_table_mutex);

	return id;
}
