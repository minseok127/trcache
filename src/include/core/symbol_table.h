#ifndef SYMBOL_TABLE_H
#define SYMBOL_TABLE_H

#include <stddef.h>
#include <pthread.h>

#include "concurrent/atomsnap.h"
#include "utils/hash_table.h"

/* Publicly visible symbol entry. */
struct public_symbol_entry {
	char *symbol_str;
	int id;
};

/*
 * public_symbol_table - Lock-free table of public symbols.
 *
 * @symbol_ptr_array_gate: Gate for snapshot versioning
 * @num_symbols:           Number of registered symbols
 * @capacity:              Allocated array capacity
 *
 * This table holds the symbol entries accessed by both user and system threads.
 * Synchronization for table expansion is handled through atomsnap.
 */
struct public_symbol_table {
	struct atomsnap_gate *symbol_ptr_array_gate;
	int num_symbols;
	int capacity;
};

/*
 * admin_symbol_entry - Admin-only symbol entry.
 *
 * @pub_symbol_entry_ptr: Corresponding public_symbol_entry
 *
 * This is a data structure accessed only by the admin thread. It holds
 * statistics and pointers associated with a single symbol. It also contains a
 * pointer to the corresponding public symbol entry, allowing the admin thread
 * to access the public symbol entry without additional synchronization with
 * users.
 */
struct admin_symbol_entry {
	struct public_symbol_entry *pub_symbol_entry_ptr;
};

/*
 * admin_symbol_table - Admin-only symbol table.
 *
 * @symbol_ptr_array: Array of admin entries
 * @num_symbols:      Number of entries
 * @capacity:         Allocated array capacity
 *
 * This is a symbol table accessed only by the admin thread. The admin thread
 * continuously monitors the number of symbols in the public symbol table and
 * expands this table accordingly. Since only the admin thread accesses it, no
 * synchronization is needed during expansion.
 */
struct admin_symbol_table {
	struct admin_symbol_entry **symbol_ptr_array;
	int num_symbols;
	int capacity;
};

/*
 * symbol_table - Combined public/admin symbol table abstraction.
 *
 * @ht_hash_table_mutex: Protects registration path
 * @symbol_id_map:       Hash table that maps from string to ID
 * @pub_symbol_table:    Lock-free reader table
 * @admin_symbol_table:  Admin-thread-only table
 * @next_symbol_id:      Next ID to assign
 *
 * This is an abstracted table that combines the public and admin symbol tables.
 * It manages symbol IDs issued during user symbol registration using a hash
 * table. Since symbol registration rarely occurs after system initialization,
 * synchronization is handled with a simple mutex.
 */
struct symbol_table {
	pthread_mutex_t ht_hash_table_mutex;
	struct ht_hash_table *symbol_id_map;
	struct public_symbol_table *pub_symbol_table;
	struct admin_symbol_table *admin_symbol_table;
	int next_symbol_id;
};

/**
 * @brief Create a new symbol_table.
 *
 * @param initial_capacity:	Initial bucket/array size (rounded up to power of two).
 *
 * @return Pointer to a newly allocated symbol_table, or NULL on error.
 *
 * @thread-safety Single-threaded: must be called before any concurrent access.
 */
struct symbol_table *symbol_table_init(int initial_capacity);

/**
 * @brief Destroy a symbol_table and free all resources.
 *
 * @param symbol_table:	Pointer returned by init_symbol_table().
 *
 * @thread-safety Single-threaded: ensure no other threads are using the table.
 */
void symbol_table_destroy(struct symbol_table *symbol_table);

/**
 * @brief Lookup a public symbol by ID.
 *
 * Lock-free, reader-safe via atomsnap.
 *
 * @param table:     Pointer to symbol_table.
 * @param symbol_id: Symbol ID to lookup.
 *
 * @return Pointer to public_symbol_entry, or NULL if out of range.
 *
 * @thread-safety Safe for concurrent readers.
 */
struct public_symbol_entry *symbol_table_lookup_public_entry(
	struct symbol_table *table, int symbol_id);

/**
 * @brief Lookup symbol ID by its string name.
 *
 * Performs a mutex-protected hash lookup.
 *
 * @param table:      Pointer to symbol_table.
 * @param symbol_str: NULL-terminated symbol string.
 *
 * @return Symbol ID >=0 on success, or -1 if not found.
 *
 * @thread-safety Safe for concurrent callers; protected by internal mutex.
 */
int symbol_table_lookup_symbol_id(
	struct symbol_table *table, const char *symbol_str);

/**
 * @brief Register a new symbol or return existing ID.
 *
 * Inserts the string into the internal hash map and expands 
 * public symbol table via copy-on-write if needed.
 *
 * @param table:      Pointer to symbol_table.
 * @param symbol_str: NULL-terminated symbol string.
 *
 * @return Assigned symbol ID >=0, or -1 on error.
 *
 * @thread-safety Safe for concurrent callers; registration path is mutex-protected.
 */
int symbol_table_register(struct symbol_table *table, const char *symbol_str);

#endif /* SYMBOL_TABLE_H */
