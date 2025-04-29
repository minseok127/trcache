#ifndef HASH_TABLE_CALLBACKS_H
#define HASH_TABLE_CALLBACKS_H

#include <stdint.h>
#include <stddef.h>

void *duplicate_symbol_str(const void *symbol_str, size_t len);

int compare_symbol_str(const void *symbol_str_1, size_t len1,
	const void *symbol_str_2, size_t len2);

void free_symbol_str(const void *symbol_str, size_t len);

uint64_t murmur_hash(const void *key, size_t len, uint64_t seed);

#endif /* HASH_TABLE_CALLBACKS_H */
