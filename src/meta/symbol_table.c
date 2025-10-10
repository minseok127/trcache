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

/**
 * @brief   atomsnap version allocation callback.
 *
 * Allocates a new atomsnap_version and its underlying array of
 * symbol_entry pointers.
 *
 * @param   arg:  Capacity for the pointer array (cast via (uint64_t)arg).
 *
 * @return  Pointer to initialized atomsnap_version, or NULL on error.
 */
static struct atomsnap_version *symbol_array_version_alloc(void *arg) {
	struct atomsnap_version *version = malloc(sizeof(struct atomsnap_version));
	uint64_t capacity = (uint64_t)arg;

	if (version == NULL) {
		errmsg(stderr, "#atomsnap_version allocation failed\n");
		return NULL;
	}

	/* Allocate symbol_entry pointer array */
	version->object = malloc(capacity * sizeof(void *));
	if (version->object == NULL) {
		errmsg(stderr, "Allocation of array is failed\n");
		free(version);
		return NULL;
	}

	version->free_context = NULL;

	/* atomsnap_make_version initializes gate and opaque */
	return version;
}

/**
 * @brief   atomsnap version free callback.
 *
 * Frees the object array and the version struct itself.
 *
 * @param   version: atomsnap_version to free.
 */
static void symbol_array_version_free(struct atomsnap_version *version) {
	struct symbol_entry **symbol_array
		= (struct symbol_entry **) version->object;
	free(symbol_array);
	free(version);
}

/**
 * @brief   Create a new symbol_table.
 *
 * @param   initial_capacity: Initial bucket/array size (power of two).
 *
 * @return  Pointer to a newly allocated symbol_table, or NULL on error.
 *
 * @thread-safety Single-threaded: must be called before any concurrent access.
 */
struct symbol_table *symbol_table_init(int initial_capacity)
{
	struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = symbol_array_version_alloc,
		.atomsnap_free_impl = symbol_array_version_free
	};
	struct atomsnap_version *symbol_ptr_array_version = NULL;
	struct symbol_table *table = calloc(1, sizeof(struct symbol_table));

	if (table == NULL) {
		errmsg(stderr, "#symbol_table allocation failed\n");
		return NULL;
	}

	pthread_mutex_init(&table->ht_hash_table_mutex, NULL);

	/* Create ID map with string callbacks */
	table->symbol_id_map = ht_create(
		1024,					/* initial capacity */
		0xDEADBEEFULL,			/* seed */
		murmur_hash,			/* hash function */
		compare_symbol_str,		/* cmp_func */
		duplicate_symbol_str,	/* dup_func */
		free_symbol_str			/* free_func */
	);

	if (table->symbol_id_map == NULL) {
		errmsg(stderr, "Failure on ht_create()\n");
		free(table);
		return NULL;
	}

	/* Create gate for version management */
	table->symbol_ptr_array_gate = atomsnap_init_gate(&ctx);
	if (table->symbol_ptr_array_gate == NULL) {
		errmsg(stderr, "Failure on atomsnap_init_gate()\n");
		ht_destroy(table->symbol_id_map);
		free(table);
		return NULL;
	}

	/* Install initial empty version */
	symbol_ptr_array_version = atomsnap_make_version(
		table->symbol_ptr_array_gate, (void *)(uintptr_t)initial_capacity);
	if (symbol_ptr_array_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		atomsnap_destroy_gate(table->symbol_ptr_array_gate);
		ht_destroy(table->symbol_id_map);
		free(table);
		return NULL;
	}

	atomsnap_exchange_version(table->symbol_ptr_array_gate,
		symbol_ptr_array_version);

	table->capacity = initial_capacity;
	table->num_symbols = 0;

	return table;
}

/**
 * @brief   Destroy a symbol_table and free all resources.
 *
 * @param   symbol_table:	Pointer returned by init_symbol_table().
 *
 * @thread-safety Single-threaded: ensure no other threads are using the table.
 */
void symbol_table_destroy(struct symbol_table *table)
{
	struct symbol_entry **symbol_ptr_array = NULL;
	struct atomsnap_version *version = NULL;
	struct symbol_entry *entry = NULL;

	if (table == NULL) {
		return;
	}

	ht_destroy(table->symbol_id_map);

	pthread_mutex_destroy(&table->ht_hash_table_mutex);

	version = atomsnap_acquire_version(table->symbol_ptr_array_gate);
	symbol_ptr_array = (struct symbol_entry **) version->object;
	for (int i = 0; i < table->num_symbols; i++) {
		entry = symbol_ptr_array[i];

		for (int j = 0; j < NUM_CANDLE_BASES; j++) {
			for (int k = 0; k < MAX_CANDLE_TYPES_PER_BASE; k++) {
				if (entry->candle_chunk_list_ptrs[j][k] != NULL) {
					candle_chunk_list_finalize(entry->candle_chunk_list_ptrs[j][k]);
					destroy_candle_chunk_list(entry->candle_chunk_list_ptrs[j][k]);
				}
			}
		}

		trade_data_buffer_destroy(entry->trd_buf);
		free(entry->symbol_str);
		free(entry);
	}
	atomsnap_release_version(version);

	/* This will call symbol_array_version_free() */
	atomsnap_destroy_gate(table->symbol_ptr_array_gate);

	free(table);
}

/**
 * @brief   Lookup a symbol by ID.
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
	struct symbol_table *table, int symbol_id)
{
	struct atomsnap_version *version =
		atomsnap_acquire_version(table->symbol_ptr_array_gate);
	struct symbol_entry **ptr_array = (struct symbol_entry **) version->object;
	struct symbol_entry *result = NULL;

	if (symbol_id >= 0 && symbol_id < table->num_symbols) {
		result = ptr_array[symbol_id];
	}

	atomsnap_release_version(version);

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
 * @brief   Allocate a new symbol entry.
 *
 * @param   tc:          Pointer to the #trcache.
 * @param   id:          Symbol ID to assign.
 * @param   symbol_str:  Symbol string to assign.
 *
 * @return  Pointer to entry, or NULL on failure.
 */
static struct symbol_entry *init_symbol_entry(
	struct trcache *tc, int id, const char *symbol_str)
{
	struct candle_chunk_list_init_ctx ctx = { 0, };
	struct symbol_entry *entry = calloc(1, sizeof(struct symbol_entry));
	struct candle_chunk_list *candle_chunk_list_ptr;

	if (entry == NULL) {
		errmsg(stderr, "#symbol_entry allocation failed\n");
		return NULL;
	}

	entry->symbol_str = duplicate_symbol_str(symbol_str,
		strlen(symbol_str) + 1);

	if (entry->symbol_str == NULL) {
		errmsg(stderr, "Failure on duplicate_symbol_str()\n");
		free(entry);
		return NULL;
	}

	entry->trd_buf = trade_data_buffer_init(tc);

	if (entry->trd_buf == NULL) {
		errmsg(stderr, "Allocation of trade_data_buffer is failed\n");
		free(entry->symbol_str);
		free(entry);
		return NULL;
	}

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		for (int i = 0; i < NUM_CANDLE_BASES; i++) {
			for (int j = 0; j < MAX_CANDLE_TYPES_PER_BASE; j++) {
				atomic_init(&entry->in_progress[s][i][j], -1);
			}
		}
	}

	for (int i = 0; i < NUM_CANDLE_BASES; i++) {
		for (int j = 0; j < tc->num_candle_types[i]; j++) {
			trcache_candle_type type = { .base_type = i, .type_idx = j };
			
			ctx.trc = tc;
			ctx.candle_type = type;
			ctx.symbol_id = id;

			candle_chunk_list_ptr = create_candle_chunk_list(&ctx);
			if (candle_chunk_list_ptr == NULL) {
				errmsg(stderr, "Candle chunk list allocation is failed\n");
				for (int b = 0; b < NUM_CANDLE_BASES; b++) {
					for (int t = 0; t < MAX_CANDLE_TYPES_PER_BASE; t++) {
						if (entry->candle_chunk_list_ptrs[b][t] != NULL) {
							destroy_candle_chunk_list(entry->candle_chunk_list_ptrs[b][t]);
						}
					}
				}
				trade_data_buffer_destroy(entry->trd_buf);
				free(entry->symbol_str);
				free(entry);
				return NULL;
			}
			entry->candle_chunk_list_ptrs[i][j] = candle_chunk_list_ptr;
		}
	}

	entry->id = id;

	return entry;
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
	struct symbol_entry **symbol_array = NULL;
	struct atomsnap_version *version = NULL, *new_version = NULL;
	int id, oldcap, newcap;

	pthread_mutex_lock(&table->ht_hash_table_mutex);

	id = table->num_symbols;

	if (ht_insert(table->symbol_id_map, symbol_str,
			strlen(symbol_str) + 1, /* string + NULL */
			(void *)(uintptr_t)id) < 0) {
		errmsg(stderr, "Failure on ht_insert()\n");
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	version = atomsnap_acquire_version(table->symbol_ptr_array_gate);
	symbol_array = (struct symbol_entry **) version->object;
	oldcap = table->capacity;

	/* Resize the array (CoW) */
	if (id >= oldcap) {
		newcap = oldcap * 2;

		new_version = atomsnap_make_version(
			table->symbol_ptr_array_gate, (void *)(uintptr_t)newcap);

		if (new_version == NULL) {
			errmsg(stderr, "Failure on atomsnap_make_version()\n");
			pthread_mutex_unlock(&table->ht_hash_table_mutex);
			return -1;
		}

		memcpy((struct symbol_entry **) new_version->object,
			(struct symbol_entry **) version->object,
			oldcap * sizeof(struct symbol_entry *));

		table->capacity = newcap;

		symbol_array = (struct symbol_entry **) new_version->object;

		atomsnap_exchange_version(table->symbol_ptr_array_gate, new_version);
	}

	symbol_array[id] = init_symbol_entry(tc, id, symbol_str);

	if (symbol_array[id] == NULL) {
		atomsnap_release_version(version);
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	table->num_symbols++;

	atomsnap_release_version(version);

	pthread_mutex_unlock(&table->ht_hash_table_mutex);

	return id;
}
