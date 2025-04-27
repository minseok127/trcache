#include <stdint.h>
#include <stddef.h>
#include <string.h>

#include "utils/hash_func.h"

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
