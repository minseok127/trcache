/*
 * @file utils/hash_table_callbacks.c
 * @brief Default callbacks for string keys and MurmurHash.
 */

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "utils/hash_table_callbacks.h"

/**
 * @brief Duplicate a string key.
 *
 * @param key: Pointer to original string.
 * @param len: Length of string in bytes (including '\0').
 *
 * @return Newly allocated copy, or NULL on malloc failure.
 */
void *duplicate_symbol_str(const void *symbol_str, size_t len)
{
	char *symbol_str_dup = malloc(len);

	if (symbol_str_dup == NULL) {
		fprintf(stderr, "duplicate_symbol_str: malloc failed\n");
		return NULL;
	}

	memcpy(symbol_str_dup, symbol_str, len);
	return symbol_str_dup;
}

/**
 * @brief Compare two string keys.
 *
 * @param k1:  First string pointer.
 * @param l1:  Length of first string.
 * @param k2:  Second string pointer.
 * @param l2:  Length of second string.
 *
 * @return 1 if lengths equal and content matches, else 0.
 */
int compare_symbol_str(const void *k1, size_t l1,
			 const void *k2, size_t l2)
{
	if (l1 != l2)
		return 0;
	return (memcmp(k1, k2, l1) == 0);
}

/**
 * @brief Free a duplicated string key.
 *
 * @param key: Pointer returned by duplicate_symbol_str.
 * @param len: Unused length parameter.
 */
void free_symbol_str(const void *key, size_t len)
{
	(void)len;
	free((void *)key);
}

/**
 * @brief 64-bit MurmurHash implementation.
 *
 * @param key: Pointer to data to hash.
 * @param len: Length of data in bytes.
 * @param seed: 32-bit seed for randomized hashing.
 *
 * @return 64-bit hash value.
 */
uint64_t murmur_hash(const void *key, size_t len, uint64_t seed)
{
	const uint64_t m = 0xc6a4a7935bd1e995ULL;
	const int r = 47;

	uint64_t h = seed ^ (len * m);

	const uint8_t *data = (const uint8_t *)key;
	const uint8_t *end = data + len;

	while (data + 8 <= end) {
		uint64_t k;
		memcpy(&k, data, sizeof(k));

		k *= m;
		k ^= k >> r;
		k *= m;

		h ^= k;
		h *= m;

		data += 8;
	}

	uint64_t tail = 0;
	switch (end - data) {
		case 7: tail ^= (uint64_t)data[6] << 48;
		case 6: tail ^= (uint64_t)data[5] << 40;
		case 5: tail ^= (uint64_t)data[4] << 32;
		case 4: tail ^= (uint64_t)data[3] << 24;
		case 3: tail ^= (uint64_t)data[2] << 16;
		case 2: tail ^= (uint64_t)data[1] <<  8;
		case 1: tail ^= (uint64_t)data[0];
				tail *= m;
				h ^= tail;
	}

	h ^= h >> r;
	h *= m;
	h ^= h >> r;

	return h;
}
