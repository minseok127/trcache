#ifndef HASH_TABLE_CALLBACKS_H
#define HASH_TABLE_CALLBACKS_H

#include <stdint.h>
#include <stddef.h>

/**
 * @brief Duplicate a string key.
 *
 * Allocates len bytes and copies the content.
 *
 * @param symbol_str:  Pointer to original string data.
 * @param len:         Length of string in bytes (incl. '\0').
 *
 * @return Newly allocated copy on success, or NULL on failure.
 */
void *duplicate_symbol_str(const void *symbol_str, size_t len);

/**
 * @brief Compare two string keys.
 *
 * @param symbol_str_1: Pointer to first string.
 * @param len1:         Length of first string.
 * @param symbol_str_2: Pointer to second string.
 * @param len2:         Length of second string.
 *
 * @return Non-zero if equal, zero if not.
 */
int compare_symbol_str(const void *symbol_str_1, 
	size_t len1,
	const void *symbol_str_2,
	size_t len2);

/**
 * @brief Free a duplicated string key.
 *
 * @param symbol_str: Pointer returned by duplicate_symbol_str.
 * @param len:        Length of string in bytes (unused).
 */
void free_symbol_str(void *symbol_str, size_t len);

/**
 * @brief 64-bit MurmurHash implementation.
 *
 * @param key:  Pointer to data to hash.
 * @param len:  Length of data in bytes.
 * @param seed: Hash seed for randomized hashing.
 *
 * @return 64-bit hash value.
 */
uint64_t murmur_hash(const void *key, size_t len, uint64_t seed);

#endif /* HASH_TABLE_CALLBACKS_H */
