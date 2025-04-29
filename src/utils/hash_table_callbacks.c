#include <stdint.h>
#include <stddef.h>
#include <string.h>

#include "utils/hash_table_callbacks.h"

/*
 * Duplicate a string key by allocating len bytes and copying.
 * Returns NULL on allocation failure.
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

/*
 * Compare two string keys of given lengths. Return 1 if equal.
 */
int compare_symbol_str(const void *symbol_str_1, size_t len1,
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
void free_symbol_str(const void *symbol_str,
	size_t len __attribute__((unused)))
{
	free(symbol_str);
}

/*
 * Murmur hash function.
 */
uint64_t murmur_hash(const void *key, size_t len, uint32_t seed)
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
