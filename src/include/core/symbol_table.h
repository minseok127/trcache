#ifndef SYMBOL_TABLE_H
#define SYMBOL_TABLE_H

#include <pthread.h>

#include "concurrent/atomsnap.h"
#include "utils/hash_table.h"

struct public_symbol_entry {
	int id;
};

/*
 * This table holds the symbol entries accessed by both user and system threads.
 * Synchronization for table expansion is handled through atomsnap.
 */
struct public_symbol_table {
	struct atomsnap_gate *symbol_array_gate;
	int num_symbols;
	int capacity;
};

/*
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
 * This is a symbol table accessed only by the admin thread. The admin thread
 * continuously monitors the number of symbols in the public symbol table and
 * expands this table accordingly. Since only the admin thread accesses it, no
 * synchronization is needed during expansion.
 */
struct admin_symbol_table {
	struct admin_symbol_entry **symbol_array;
	int num_symbols;
	int capacity;
};

/*
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

struct symbol_table *init_symbol_table(int initial_capacity);

void destroy_symbol_table(struct symbol_table *symbol_table);

struct public_symbol_entry *symbol_table_lookup_public_entry(
	struct symbol_table *table, int symbol_id);

int symbol_table_register(struct symbol_table *table,
	const char *symbol_str, size_t symbol_str_len);

#endif /* SYMBOL_TABLE_H */
