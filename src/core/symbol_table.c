#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "core/symbol_table.h"
#include "utils/hash_func.h"

#include "trcache.h"

/* Callback function used in the symbol id hash table */
static void *duplicate_symbol_str(const void *symbol_str, size_t len)
{
	char *symbol_str_dup = malloc(len);
	memcpy(symbol_str_dup, symbol_str, len);
	return symbol_str_dup;
}

/* Callback function used in the symbol id hash table */
static int compare_symbol_str(const void *symbol_str_1, size_t len1,
	const void *symbol_str_2, size_t len2)
{
	if (len1 != len2) {
		return 0;
	}

	return (memcmp(symbol_str_1, symbol_str_2, len1) == 0);
}

/* Callback function used in the symbol id hash table */
static void free_symbol_str(const void *symbol_str,
	size_t len __attribute__((unused)))
{
	free(symbol_str);
}

/* Callback function for atomsnap used in public symbol table synchronization */
static struct atomsnap_version *symbol_array_version_alloc(void *arg) {
	struct atomsnap_version *version = malloc(sizeof(struct atomsnap_version));
	uint64_t capacity = (uint64_t)arg;

	if (version == NULL) {
		fprintf("symbol_array_version_alloc: malloc failed\n");
		return NULL;
	}

	/* Allocate public_symbol_entry pointer array */
	version->object = malloc(capacity * sizeof(void *));
	if (version->object == NULL) {
		fprintf("symbol_array_version_alloc: array malloc failed\n");
		free(version);
		return NULL;
	}

	version->free_context = NULL;

    return version;
}

/* Callback function for atomsnap used in public symbol table synchronization */
static void symbol_array_version_free(struct atomsnap_version *version) {
    struct public_symbol_entry **symbol_array
		= (struct public_symbol_entry **) version->object;
    free(symbol_array);
    free(version);
}

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
		fprintf(stderr, "init_public_symbol_tabe: atomsnap_init_gate failed\n");
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

static void
destory_public_symbol_table(struct public_symbol_table *table)
{
	atomsnap_destroy(table->symbol_array_gate);
	free(table);
}

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

static void
destory_admin_symbol_table(struct admin_symbol_table *table)
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

void destory_symbol_table(struct symbol_table *table)
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
