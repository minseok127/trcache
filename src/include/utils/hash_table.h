#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/**
 * @brief Opaque hash table type.
 */
typedef struct ht_hash_table ht_hash_table;

/**
 * @brief Prototype for hashing keys.
 *
 * @param key:   Pointer to key data.
 * @param len:   Length of key in bytes.
 * @param seed:  Hash seed for randomized hashing.
 *
 * @return 64-bit hash value.
 */
typedef uint64_t (*ht_hash_func)(const void *key, size_t len, uint64_t seed);

/**
 * @brief Prototype for comparing keys.
 *
 * @param k1: Pointer to first key.
 * @param l1: Length of first key.
 * @param k2: Pointer to second key.
 * @param l2: Length of second key.
 *
 * @return Non-zero if keys equal, zero otherwise.
 */
typedef int (*ht_cmp_key_func)(const void *key1, size_t len1,
	const void *key2, size_t len2);

/**
 * @brief Prototype for duplicating keys.
 *
 * @param key: Pointer to key data.
 * @param len: Length of key in bytes.
 *
 * @return Newly allocated key copy, or NULL on failure.
 */
typedef void *(*ht_dup_key_func)(const void *key, size_t len);

/**
 * @brief Prototype for freeing duplicated keys.
 *
 * @param key: Pointer returned by dup_func.
 * @param len: Length of key in bytes.
 */
typedef void (*ht_free_key_func)(void *key, size_t len);

/**
 * @brief Create a new hash table.
 *
 * @param initial_capacity: Minimum bucket count (rounded up).
 * @param seed:             Hash seed value.
 * @param hash_func:        User hash function, or NULL for default.
 * @param cmp_func:         User compare function, or NULL for default.
 * @param dup_func:         User dup function, or NULL for default.
 * @param free_func:        User free function, or NULL for default.
 8
 * @return Pointer to new table, or NULL on failure.
 */
struct ht_hash_table *ht_create(size_t initial_capacity,
	uint64_t seed,
	ht_hash_func hash_func,
	ht_cmp_key_func cmp_func,
	ht_dup_key_func dup_func,
	ht_free_key_func free_func);

/**
 * @brief Destroy a hash table and free all resources.
 *
 * @param table: Pointer from ht_create().
 */
void ht_destroy(ht_hash_table *table);

/**
 * @brief Insert or update a key–value pair.
 *
 * @param t:     Pointer to hash table.
 * @param key:   Pointer to key data.
 * @param len:   Length of key in bytes.
 * @param value: Pointer to associated value.
 *
 * @return 0 on success, -1 on failure.
 */
int ht_insert(ht_hash_table *t, const void *key, size_t len, void *value);

/**
 * @brief Lookup a value by key.
 *
 * @param t:     Pointer to hash table.
 * @param key:   Pointer to key data.
 * @param len:   Length of key in bytes.
 * @param found: Output flag set to true if found.
 *
 * @return Pointer to value, or NULL if not found.
 */
void *ht_find(const ht_hash_table *t, const void *key, size_t len, bool *found);

/**
 * @brief Remove a key–value pair.
 *
 * @param t:   Pointer to hash table.
 * @param key: Pointer to key data.
 * @param len: Length of key in bytes.
 */
void ht_remove(ht_hash_table *t, const void *key, size_t len);

#endif /* HASH_TABLE_H */
