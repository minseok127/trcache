/**
 * @file core/symbol_table.c
 * @brief Implements a thread-safe symbol table with lock-free reads by atomsnap
 *        and mutex-protected copy-on-write updates for writes.
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "core/symbol_table.h"
#include "utils/hash_table_callbacks.h"
#include "utils/log.h"

#include "trcache.h"

/**
 * @brief atomsnap version allocation callback.
 *
 * Allocates a new atomsnap_version and its underlying array of
 * public_symbol_entry pointers.
 *
 * @param arg:  Capacity for the pointer array (cast via (uint64_t)arg).
 *
 * @return Pointer to initialized atomsnap_version, or NULL on error.
 */
static struct atomsnap_version *symbol_array_version_alloc(void *arg) {
	struct atomsnap_version *version = malloc(sizeof(struct atomsnap_version));
	uint64_t capacity = (uint64_t)arg;

	if (version == NULL) {
		errmsg(stderr, "#atomsnap_version allocation failed\n");
		return NULL;
	}

	/* Allocate public_symbol_entry pointer array */
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
 * @brief atomsnap version free callback.
 *
 * Frees the object array and the version struct itself.
 *
 * @param version: atomsnap_version to free.
 */
static void symbol_array_version_free(struct atomsnap_version *version) {
	struct public_symbol_entry **symbol_array
		= (struct public_symbol_entry **) version->object;
	free(symbol_array);
	free(version);
}

/**
 * @brief Initialize public symbol table.
 *
 * Creates atomsnap_gate and installs initial empty snapshot.
 *
 * @param initial_capacity: Initial array capacity.
 *
 * @return Pointer to public_symbol_table, or NULL on error.
 */
static struct public_symbol_table *
init_public_symbol_table(int initial_capacity)
{
	struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = symbol_array_version_alloc,
		.atomsnap_free_impl = symbol_array_version_free
	};

	struct atomsnap_version *symbol_ptr_array_version = NULL;

	struct public_symbol_table *table
		= malloc(sizeof(struct public_symbol_table));

	if (table == NULL) {
		errmsg(stderr, "#public_symbol_table allocation failed\n");
		return NULL;
	}

	table->num_symbols = 0;
	table->capacity = initial_capacity;

	/* Create gate for version management */
	table->symbol_ptr_array_gate = atomsnap_init_gate(&ctx);
	if (table->symbol_ptr_array_gate == NULL) {
		errmsg(stderr, "Failure on atomsnap_init_gate()\n");
		free(table);
		return NULL;
	}

	/* Install initial empty version */
	symbol_ptr_array_version = atomsnap_make_version(
		table->symbol_ptr_array_gate, (void *)(uintptr_t)initial_capacity);
	if (symbol_ptr_array_version == NULL) {
		errmsg(stderr, "Failure on atomsnap_make_version()\n");
		atomsnap_destroy_gate(table->symbol_ptr_array_gate);
		free(table);
		return NULL;
	}

	atomsnap_exchange_version(table->symbol_ptr_array_gate,
		symbol_ptr_array_version);

	return table;
}

/**
 * @brief Destroy public_symbol_table and its gate.
 *
 * @param table: public_symbol_table to destroy.
 */
static void
destroy_public_symbol_table(struct public_symbol_table *table)
{
	struct public_symbol_entry **symbol_ptr_array = NULL;
	struct atomsnap_version *version = NULL;

	if (table == NULL) {
		return;
	}

	version = atomsnap_acquire_version(table->symbol_ptr_array_gate);
	symbol_ptr_array = (struct public_symbol_entry **) version->object;
	for (int i = 0; i < table->num_symbols; i++) {
		free(symbol_ptr_array[i]->symbol_str);
		free(symbol_ptr_array[i]);
	}
	atomsnap_release_version(version);

	/* This will call symbol_array_version_free() */
	atomsnap_destroy_gate(table->symbol_ptr_array_gate);
	free(table);
}

/**
 * @brief Initialize admin symbol table.
 *
 * Allocates array of admin entries.
 *
 * @param initial_capacity: Number of entries.
 *
 * @return Pointer to admin_symbol_table, or NULL on error.
 */
static struct admin_symbol_table *
init_admin_symbol_table(int initial_capacity)
{
	struct admin_symbol_table *table = malloc(sizeof(struct admin_symbol_table));

	if (table == NULL) {
		errmsg(stderr, "#admin_symbol_table allocation failed\n");
		return NULL;
	}

	table->num_symbols = 0;
	table->capacity = initial_capacity;
	table->symbol_ptr_array = calloc(1, initial_capacity * sizeof(void *));

	if (table->symbol_ptr_array == NULL) {
		errmsg(stderr, "Allocation of symbol_ptr_array is failed\n");
		free(table);
		return NULL;
	}

	return table;
}

/**
 * @brief Destroy admin_symbol_table.
 *
 * @param table: admin_symbol_table to destroy.
 */
static void
destroy_admin_symbol_table(struct admin_symbol_table *table)
{
	if (table == NULL) {
		return;
	}

	for (int i = 0; i < table->num_symbols; i++) {
		free(table->symbol_ptr_array[i]);
	}

	free(table->symbol_ptr_array);
	free(table);
}

/**
 * @brief Create and initialize symbol_table.
 *
 * @param initial_capacity: Initial size for public/admin tables.
 *
 * @return Pointer to symbol_table, or NULL on error.
 */
struct symbol_table *symbol_table_init(int initial_capacity)
{
	struct symbol_table *table = calloc(1, sizeof(struct symbol_table));

	if (table == NULL) {
		errmsg(stderr, "#symbol_table allocation failed\n");
		return NULL;
	}

	pthread_mutex_init(&table->ht_hash_table_mutex, NULL);

	table->next_symbol_id = 0;

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

	table->pub_symbol_table = init_public_symbol_table(initial_capacity);
	
	if (table->pub_symbol_table == NULL) {
		errmsg(stderr, "Failure on init_public_symbol_table()\n");
		ht_destroy(table->symbol_id_map);
		free(table);
		return NULL;
	}

	table->admin_symbol_table = init_admin_symbol_table(initial_capacity);
	
	if (table->admin_symbol_table == NULL) {
		errmsg(stderr, "Failure on init_admin_symbol_table()\n");
		ht_destroy(table->symbol_id_map);
		destroy_public_symbol_table(table->pub_symbol_table);
		free(table);
		return NULL;
	}

	return table;
}

/**
 * @brief Destroy symbol_table and all substructures.
 *
 * @param table: symbol_table to destroy.
 */
void symbol_table_destroy(struct symbol_table *table)
{
	if (table == NULL) {
		return;
	}

	ht_destroy(table->symbol_id_map);

	pthread_mutex_destroy(&table->ht_hash_table_mutex);

	destroy_public_symbol_table(table->pub_symbol_table);

	destroy_admin_symbol_table(table->admin_symbol_table);

	free(table);
}

/**
 * @brief Lock-free lookup of public_symbol_entry by ID.
 *
 * @param table: symbol_table pointer.
 * @param symbol_id: ID to lookup.
 *
 * @return public_symbol_entry or NULL if out of range.
 */
struct public_symbol_entry *
symbol_table_lookup_public_entry(struct symbol_table *table, int symbol_id)
{
	struct public_symbol_table *pub_symbol_table = table->pub_symbol_table;
	struct atomsnap_version *version =
		atomsnap_acquire_version(pub_symbol_table->symbol_ptr_array_gate);
	struct public_symbol_entry **ptr_array =
		(struct public_symbol_entry **) version->object;
	struct public_symbol_entry *result = NULL;

	if (symbol_id >= 0 && symbol_id < pub_symbol_table->num_symbols) {
		result = ptr_array[symbol_id];
	}

	atomsnap_release_version(version);

	return result;
}

/**
 * @brief Lookup symbol ID by string.
 *
 * @param table: symbol_table pointer.
 * @param symbol_str: NULL-terminated string.
 *
 * @return ID >=0, or -1 if not found.
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
 * @brief Allocate a new public symbol entry.
 *
 * @param id:         Symbol ID to assign.
 * @param symbol_str: Symbol string to assign.
 *
 * @return Pointer to entry, or NULL on failure.
 */
static struct public_symbol_entry *init_public_symbol_entry(int id, 
	const char *symbol_str)
{
	struct public_symbol_entry *entry = malloc(
		sizeof(struct public_symbol_entry));

	if (entry == NULL) {
		errmsg(stderr, "#public_symbol_entry allocation failed\n");
		return NULL;
	}

	entry->id = id;

	entry->symbol_str = duplicate_symbol_str(symbol_str,
		strlen(symbol_str) + 1);

	if (entry->symbol_str == NULL) {
		errmsg(stderr, "Failure on duplicate_symbol_str()\n");
		free(entry);
		return NULL;
	}

	return entry;
}

/**
 * @brief Register a new symbol string.
 *
 * @param table: symbol_table pointer.
 * @param symbol_str: NULL-terminated string.
 *
 * @return Assigned ID >=0, or -1 on error.
 */
int symbol_table_register(struct symbol_table *table, const char *symbol_str)
{
	struct public_symbol_table *pub_symbol_table = table->pub_symbol_table;
	struct public_symbol_entry **symbol_array = NULL;
	struct atomsnap_version *version = NULL, *new_version = NULL;
	int id, oldcap, newcap;

	pthread_mutex_lock(&table->ht_hash_table_mutex);

	id = table->next_symbol_id;

	if (ht_insert(table->symbol_id_map, symbol_str,
			strlen(symbol_str) + 1, /* string + NULL */
			(void *)(uintptr_t)id) < 0) {
		errmsg(stderr, "Failure on ht_insert()\n");
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	version = atomsnap_acquire_version(pub_symbol_table->symbol_ptr_array_gate);
	symbol_array = (struct public_symbol_entry **) version->object;
	oldcap = pub_symbol_table->capacity;

	/* Resize the array (CoW) */
	if (id >= oldcap) {
		newcap = oldcap * 2;

		new_version = atomsnap_make_version(
			pub_symbol_table->symbol_ptr_array_gate, (void *)(uintptr_t)newcap);

		if (new_version == NULL) {
			errmsg(stderr, "Failure on atomsnap_make_version()\n");
			pthread_mutex_unlock(&table->ht_hash_table_mutex);
			return -1;
		}

		memcpy((struct public_symbol_entry **) new_version->object,
				(struct public_symbol_entry **) version->object,
				oldcap * sizeof(struct public_symbol_entry *));

		pub_symbol_table->capacity = newcap;

		symbol_array = (struct public_symbol_entry **) new_version->object;

		atomsnap_exchange_version(
			pub_symbol_table->symbol_ptr_array_gate, new_version);
	}

	symbol_array[id] = init_public_symbol_entry(id, symbol_str);

	if (symbol_array[id] == NULL) {
		atomsnap_release_version(version);
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	pub_symbol_table->num_symbols++;

	table->next_symbol_id++;

	atomsnap_release_version(version);

	pthread_mutex_unlock(&table->ht_hash_table_mutex);

	return id;
}
