/**
 * @file utils/htab.c
 * @brief Generic hash table implementation with customizable callbacks.
 *
 * Provides functions to create, destroy, insert, lookup, and remove key–value pairs.
 * User supplies hash, compare, duplicate, and free callbacks to control key behavior.
 * Automatically resizes when load factor exceeds threshold.
 * Not thread-safe: callers must synchronize externally for concurrent access.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "utils/hash_table.h"

#define HT_MIN_CAP 8			/* Minimum bucket count (power of two) */
#define HT_LOAD_FACTOR_NUM 3	/* Load factor numerator (3/4) */
#define HT_LOAD_FACTOR_DEN 4	/* Load factor denominator (4/4) */

/*
 * ht_item - Single bucket item holding a key, length, value, and next pointer.
 *
 * @key:	Pointer to key data (or 8-byte scalar)
 * @len:	Length of key in bytes
 * @value:	Associated value pointer (or 8-byte scalar)
 * @next:	Next item in bucket chain
 */
struct ht_item {
	void *key;
	size_t len;
	void *value;
	struct ht_item *next;
};

/*
 * ht_hash_table - Hash table structure.
 *
 * @buckets:	Array of bucket heads (size = capacity)
 * @hash_func:	User-provided hash function
 * @cmp_func:	User-provided key comparison function
 * @dup_func:	User-provided key duplication function
 * @free_func:	User-provided key free function
 * @capacity:	Number of buckets (power of two)
 * @size:		Number of stored key–value pairs
 * @seed:		Hash seed for randomized hashing
 */
struct ht_hash_table {
	struct ht_item **buckets;
	ht_hash_func hash_func; /* hash function */
	ht_cmp_key_func cmp_func; /* key compare function */
	ht_dup_key_func dup_func; /* key duplicate function */
	ht_free_key_func free_func; /* key free function */
	size_t capacity;   /* # buckets */
	size_t size;       /* # stored pairs */
	uint64_t seed;
};

/**
 * @internal
 * @brief Compute bucket index for a given key.
 * @param t:	Hash table pointer.
 * @param key:	Key pointer.
 * @param len:	Key length.
 * @return Index in [0, capacity).
 */
static size_t ht_index(const struct ht_hash_table *t, void *key, size_t len)
{
	uint32_t h = t->hash_func(key, len, t->seed);
	return (size_t)(h & (t->capacity - 1)); /* capacity always power of two */
}

/**
 * @internal
 * @brief Resize table to at least new_cap buckets (rounded up).
 * @param t:		Hash table pointer.
 * @param new_cap:	Desired minimum capacity.
 * @return 0 on success, -1 on failure.
 */
static int ht_resize(struct ht_hash_table *t, size_t new_cap)
{
	struct ht_item **new_buckets = NULL;
	struct ht_item *it = NULL, *next = NULL;
	size_t cap = 1, idx = -1;

	if (new_cap < HT_MIN_CAP) {
		new_cap = HT_MIN_CAP;
	}

	/* round to power of two */
	while (cap < new_cap) {
		cap <<= 1;
	}
	t->capacity = cap;

	new_buckets = calloc(cap, sizeof(struct ht_item *));
	if (!new_buckets) {
		return -1;
	}

	/* Rehash all items */
	for (size_t i = 0; i < t->capacity; ++i) {
		it = t->buckets[i];
		while (it) {
			next = it->next;
			idx = ht_index(t, it->key, it->len);
			it->next = new_buckets[idx];
			new_buckets[idx] = it;
			it = next;
		}
	}
	free(t->buckets);
	t->buckets  = new_buckets;
	return 0;
}

/* Default callback implementations assume key is uint64_t scalar */
static uint64_t default_hash_func(const void *key,
	size_t len __attribute__((unused)),
	uint64_t seed __attribute__((unused)))
{
    return (uint64_t)key * 0x9e3779b97f4a7c15ull;
}
static int default_cmp_func(const void *key1,
	size_t len1 __attribute__((unused)), const void *key2,
	size_t len2 __attribute__((unused)))
{
	return ((uint64_t)key1 == (uint64_t)key2);
}
static void *default_dup_func(const void *key,
	size_t len __attribute__((unused)))
{
	return key;
}
static void default_free_func(void *key, size_t len)
{
}

/**
 * @brief Create a new hash table.
 * @param initial_capacity:	Minimum bucket count
 *                          (will be rounded up to a power of two).
 * @param seed:       Hash seed value for user hash_func.
 * @param hash_func:  User-provided hash function (or NULL for default).
 * @param cmp_func:   User-provided key compare function
 *                    (or NULL for default).
 * @param dup_func:   User-provided key duplication function
 *                    (or NULL for default).
 * @param free_func:  User-provided key free function
 *                    (or NULL for default).
 * @return Pointer to new table, or NULL on allocation failure.
 */
struct ht_hash_table *ht_create(size_t initial_capacity, uint64_t seed,
	ht_hash_func hash_func, ht_cmp_key_func cmp_func,
	ht_dup_key_func dup_func, ht_free_key_func free_func)
{
	struct ht_hash_table *t = NULL;
	size_t cap = 1;

	if (initial_capacity < HT_MIN_CAP) {
		initial_capacity = HT_MIN_CAP;
	}

	/* Ensure power of two */
	while (cap < initial_capacity) {
		cap <<= 1;
	}

	t = malloc(sizeof(*t));
	if (!t) {
		return NULL;
	}

	t->buckets = calloc(cap, sizeof(struct ht_item *));
	if (!t->buckets) { 
		free(t); 
		return NULL;
	}

	t->capacity = cap;
	t->size     = 0;
	t->seed     = seed;

	t->hash_func = hash_func != NULL ? hash_func : default_hash_func;
	t->cmp_func = cmp_func != NULL ? cmp_func : default_cmp_func;
	t->dup_func = dup_func != NULL ? dup_func : default_dup_func;
	t->free_func = free_func != NULL ? free_func : default_free_func;

	return t;
}

/**
 * @brief Destroy a hash table, freeing all keys and items.
 * @param t Pointer to table created by ht_create().
 */
void ht_destroy(struct ht_hash_table *t)
{
	struct ht_item *next = NULL, *it = NULL;

	if (!t) {
		return;
	}

	for (size_t i = 0; i < t->capacity; ++i) {
		it = t->buckets[i];
		while (it) {
			next = it->next;
			t->free_func(it->key, it->len);
			free(it);
			it = next;
		}
	}
	free(t->buckets);
	free(t);
}

/**
 * @brief Insert or overwrite a key–value pair.
 * @param t:     Pointer to hash table.
 * @param key:   Pointer to key data.
 * @param len:   Length of key in bytes.
 * @param value: Pointer to associated value.
 * @return 0 on success, -1 on failure.
 */
int ht_insert(struct ht_hash_table *t, const void *key, size_t len, void *value)
{
	struct ht_item *it = NULL, *node = NULL;
	size_t idx = -1;

	/* Resize if load factor exceeded */
	if (t->size * HT_LOAD_FACTOR_DEN >= t->capacity * HT_LOAD_FACTOR_NUM) {
		if (ht_resize(t, t->capacity << 1) != 0) {
			fprintf(stderr, "ht_insert: ht_resize failed\n");
			return -1;
		}
	}

	idx = ht_index(t, key, len);
	it = t->buckets[idx];
	while (it) {
		if (t->cmp_func(key, len, it->key, it->len)) {
			it->value = value; /* overwrite */
			return 0;
		}
		it = it->next;
	}

	node = malloc(sizeof(*node));
	if (node == NULL) {
		fprintf(stderr, "ht_set: malloc failed\n");
		return -1;
	}

	node->key = t->dup_func(key, len);
	node->len = len;
	node->value = value;
	node->next = t->buckets[idx];

	t->buckets[idx] = node;
	t->size += 1;

	return 0;
}

/**
 * @brief Lookup a value by key.
 * @param t:     Pointer to hash table.
 * @param key:   Pointer to key data.
 * @param len:   Length of key in bytes.
 * @param found: Output flag set to true if found.
 * @return Pointer to value or NULL.
 */
void *ht_find(const struct ht_hash_table *t, const void *key,
	size_t len, bool *found)
{
	size_t idx = ht_index(t, key, len);
	struct ht_item *it = t->buckets[idx];

	while (it) {
		if (t->cmp_func(key, len, it->key, it->len)) {
			*found = true;
			return it->value;
		}
		it = it->next;
	}

	*found = false;
	return NULL;
}

/**
 * @brief Remove a key–value pair.
 * @param t:   Pointer to hash table.
 * @param key: Pointer to key data.
 * @param len: Length of key in bytes.
 */
void ht_remove(struct ht_hash_table *t, const void *key, size_t len)
{
	size_t idx = ht_index(t, key, len);
	struct ht_item *it = t->buckets[idx], *prev = NULL;

	while (it) {
		if (t->cmp_func(key, len, it->key, it->len)) {
			if (prev) {
				prev->next = it->next;
			} else {
				t->buckets[idx] = it->next;
			}

			t->free_func(it->key, it->len);
			free(it);
			t->size -= 1;
			return;
		}
		prev = it;
		it = it->next;
	}
}
