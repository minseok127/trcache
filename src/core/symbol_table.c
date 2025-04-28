#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "core/symbol_table.h"
#include "utils/hash_func.h"

#include "trcache.h"

/*
 * Duplicate a string key by allocating len bytes and copying.
 * Returns NULL on allocation failure.
 */
static void *duplicate_symbol_str(const void *symbol_str, size_t len)
{
	char *symbol_str_dup = malloc(len);

	if (symbol_str_dup == NULL) {
		fprintf(stderr, "duplicate_symbol_str: malloc failed\n");
		return NULL;
	}

	memcpy(symbol_str_dup, symbol_str, len);
	return symbol_str_dup;
}

/*
 * Compare two string keys of given lengths. Return 1 if equal.
 */
static int compare_symbol_str(const void *symbol_str_1, size_t len1,
	const void *symbol_str_2, size_t len2)
{
	if (len1 != len2) {
		return 0;
	}

	return (memcmp(symbol_str_1, symbol_str_2, len1) == 0);
}

/*
 * Free a duplicated string key.
 */
static void free_symbol_str(const void *symbol_str,
	size_t len __attribute__((unused)))
{
	free(symbol_str);
}

/*
 * atomsnap version allocation callback.
 * alloc_arg is passed as capacity of the struct public_symbol_entry pointer
 * array.
 */
static struct atomsnap_version *symbol_array_version_alloc(void *arg) {
	struct atomsnap_version *version = malloc(sizeof(struct atomsnap_version));
	uint64_t capacity = (uint64_t)arg;

	if (version == NULL) {
		fprintf(stderr, "symbol_array_version_alloc: malloc failed\n");
		return NULL;
	}

	/* Allocate public_symbol_entry pointer array */
	version->object = malloc(capacity * sizeof(void *));
	if (version->object == NULL) {
		fprintf(stderr, "symbol_array_version_alloc: array malloc failed\n");
		free(version);
		return NULL;
	}

	version->free_context = NULL;

	/* atomsnap_make_version initializes gate and opaque */
    return version;
}

/*
 * atomsnap version free callback.
 * Frees the array and version object.
 */
static void symbol_array_version_free(struct atomsnap_version *version) {
    struct public_symbol_entry **symbol_array
		= (struct public_symbol_entry **) version->object;
    free(symbol_array);
    free(version);
}

/*
 * Initialize public_symbol_table with atomsnap gate and initial
 * public_symbol_entry pointer array version.
 */
static struct public_symbol_table *
init_public_symbol_table(int initial_capacity)
{
	struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = symbol_array_version_alloc,
		.atomsnap_free_impl = symbol_array_version_free
    };

	struct atomsnap_version *symbol_array_version = NULL;

	struct public_symbol_table *table
		= malloc(sizeof(struct public_symbol_table));

	if (table == NULL) {
		fprintf(stderr, "init_public_symbol_table: malloc failed\n");
		return NULL;
	}

	table->num_symbols = 0;
	table->capacity = initial_capacity;

	table->symbol_array_gate = atomsnap_init_gate(&ctx);
	if (table->symbol_array_gate == NULL) {
		fprintf(stderr, "init_public_symbol_table: atomsnap_init_gate failed\n");
		free(table);
		return NULL;
	}

	symbol_array_version = atomsnap_make_version(table->symbol_array_gate,
		(void *)initial_capacity);
	if (symbol_array_version == NULL) {
		fprintf(stderr, "init_public_symbol_tabe: atomsnap_make_version failed\n");
		atomsnap_destroy_gate(table->symbol_array_gate);
		free(table);
		return NULL;
	}

	/* This version has an empty public_symbol_entry pointer array */
	atomsnap_exchange_version(table->symbol_array_gate, symbol_array_version);

	return table;
}

/*
 * Destroy public_symbol_table and atomsnap gate.
 */
static void
destroy_public_symbol_table(struct public_symbol_table *table)
{
	atomsnap_destroy_gate(table->symbol_array_gate);
	free(table);
}

/*
 * Initialize admin_symbol_table with given capacity.
 */
static struct admin_symbol_table *
init_admin_symbol_table(int initial_capacity)
{
	struct admin_symbol_table *table = malloc(sizeof(struct admin_symbol_table));

	if (table == NULL) {
		fprintf(stderr, "init_admin_symbol_table: malloc failed\n");
		return NULL;
	}

	table->num_symbols = 0;
	table->capacity = initial_capacity;
	table->symbol_array = calloc(1, initial_capacity * sizeof(void *));

	if (table->symbol_array == NULL) {
		fprintf(stderr, "init_admin_symbol_table: calloc failed\n");
		free(table);
		return NULL;
	}

	return table;
}

/*
 * Free admin_symbol_table resources.
 */
static void
destroy_admin_symbol_table(struct admin_symbol_table *table)
{
	free(table->symbol_array);
	free(table);
}

/*
 * Creates and initializes the symbol table structure that abstracts the public
 * and admin symbol tables. The hash table mapping symbols to symbol IDs uses
 * symbols as keys, which are variable-length strings. Therefore, callback
 * functions are defined and passed to the hash table to handle them properly.
 */
struct symbol_table *init_symbol_table(int initial_capacity)
{
	struct symbol_table *table = calloc(1, sizeof(struct symbol_table));

	if (table == NULL) {
		fprintf(stderr, "init_symbol_table: calloc failed\n");
		return NULL;
	}

	pthread_mutex_init(&table->ht_hash_table_mutex, NULL);

	table->next_symbol_id = 0;

	table->symbol_id_map = ht_create(1024, /* initial capacity */
		0xDEADBEEFULL, /* seed */
		murmur_hash, /* hash function */
		compare_symbol_str, /* cmp_func */
		duplicate_symbol_str, /* dup_func */
		free_symbol_str /* free_func */
	);

	if (table->symbol_id_map == NULL) {
		fprintf(stderr, "init_symbol_table: ht_create failed\n");
		free(table);
		return NULL;
	}

	table->pub_symbol_table = init_public_symbol_table(initial_capacity);
	
	if (table->pub_symbol_table == NULL) {
		fprintf(stderr, "init_symbol_table: init_public_symbol_table failed\n");
		ht_destroy(table->symbol_id_map);
		free(table);
		return NULL;
	}

	table->admin_symbol_table = init_admin_symbol_table(initial_capacity);
	
	if (table->admin_symbol_table == NULL) {
		fprintf(stderr, "init_symbol_table: init_admin_symbol_table failed\n");
		ht_destroy(table->symbol_id_map);
		destroy_public_symbol_table(table->pub_symbol_table);
		free(table);
		return NULL;
	}

	return table;
}

/*
 * Clean up entire symbol_table, including hash map, public and admin tables.
 */
void destroy_symbol_table(struct symbol_table *table)
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

/*
 * Lock-free lookup of public_symbol_entry by symbol_id.
 * Acquires a snapshot via atomsnap, reads entry, and releases snapshot.
 * Internally:
 *     1. atomsnap_acquire_version (outer ref++)  
 *     2. read from snapshot->object array  
 *     3. atomsnap_release_version (inner ref++ → free if zero) 
 */
struct public_symbol_entry *
symbol_table_lookup_public_entry(struct symbol_table *table, int symbol_id)
{
	struct public_symbol_table *pub_symbol_table = table->pub_symbol_table;
	struct atomsnap_version *version =
		atomsnap_acquire_version(pub_symbol_table->symbol_array_gate);
	struct public_symbol_entry **array =
		(struct public_symbol_entry **) version->object;
	struct public_symbol_entry *result = NULL;

	if (symbol_id >= 0 && symbol_id < pub_symbol_table->capacity) {
		result = array[symbol_id];
	}

	atomsnap_release_version(version);

	return result;
}

/*
 * Allocate and init public symbol entry.
 */
static struct public_symbol_entry *init_public_symbol_entry(int id)
{
	struct public_symbol_entry *entry = malloc(
		sizeof(struct public_symbol_entry));

	if (entry == NULL) {
		fprintf(stderr, "init_public_symbol_entry: malloc failed\n");
		return NULL;
	}

	entry->id = id;

	return entry;
}

/*
 * Registers a new symbol name and entry.
 * Uses a mutex to synchronize hash map insertion, then add the new entry into
 * the given index. If the array need to be resized, use atomsnap.
 * Returns the assigned symbol ID.
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
			(void *)id) < 0) {
		fprintf(stderr, "symbol_table_register: ht_insert error\n");
		pthread_mutex_unlock(&table->ht_hash_table_mutex);
		return -1;
	}

	version = atomsnap_acquire_version(pub_symbol_table->symbol_array_gate);
	symbol_array = (struct public_symbol_entry **) version->object;
	oldcap = pub_symbol_table->capacity;

	if (id >= oldcap) {
		newcap = oldcap * 2;

		new_version = atomsnap_make_version(pub_symbol_table->symbol_array_gate,
			(void *) newcap);

		if (new_version == NULL) {
			fprintf(stderr, "symbol_table_register: new_version alloc failed\n");
			pthread_mutex_unlock(&table->ht_hash_table_mutex);
			return -1;
		}

		memcpy((struct public_symbol_entry **) new_version->object,
				(struct public_symbol_entry **) version->object,
				oldcap * sizeof(struct public_symbol_entry *));

		pub_symbol_table->capacity = newcap;

		symbol_array = (struct public_symbol_entry **) new_version->object;

		atomsnap_exchange_version(pub_symbol_table->symbol_array_gate,
			new_version);
	}

	atomsnap_release_version(version);

	symbol_array[id] = init_public_symbol_entry(id);

	pub_symbol_table->num_symbols++;

	table->next_symbol_id++;

	pthread_mutex_unlock(&table->ht_hash_table_mutex);

	return id;
}
