#ifndef HASH_FUNC_H
#define HASH_FUNC_H

#include <stdint.h>
#include <stddef.h>

uint64_t murmur_hash(const void *key, size_t len, uint64_t seed);

#endif /* HASH_FUNC_H */
