#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stdint.h>
#include <stddef.h>

typedef struct ht_hash_table ht_hash_table;

typedef uint64_t (*ht_hash_func)(const void *key, size_t len, uint64_t seed);

typedef int (*ht_cmp_key_func)(const void *key1, size_t len1,
	const void *key2, size_t len2);

typedef void *(*ht_dup_key_func)(const void *key, size_t len);

typedef void (*ht_free_key_func)(void *key, size_t len);

struct ht_hash_table *ht_create(size_t initial_capacity, uint64_t seed,
	ht_hash_func hash_func, ht_cmp_key_func cmp_func,
	ht_dup_key_func dup_func, ht_free_key_func free_func);

void ht_destroy(ht_hash_table *table);

int ht_insert(ht_hash_table *t, const void *key, size_t len, void *value);

bool ht_find(const ht_hash_table *t, const void *key, size_t len, void **out);

void ht_remove(ht_hash_table *t, const void *key, size_t len);

#endif /* HASH_TABLE_H */
