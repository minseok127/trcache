#ifndef VECTOR_H
#define VECTOR_H

#include <stddef.h>
#include <stdlib.h>
#include <string.h> 

/*
 * vector - generic, single-thread dynamic array
 *
 * @data:      underlying storage (void* → cast on use)
 * @elem_size: byte-size of one element
 * @size:      number of valid elements
 * @capacity:  allocated element slots
 */
struct vector {
	void   *data;
	size_t  elem_size;
	size_t  size;
	size_t  capacity;
};

/**
 * @brief  Initialize an empty vector.
 * @param  elem_size: sizeof(one element)
 * @return Pointer to vector on success, NULL on failure.
 */
struct vector *vector_init(size_t elem_size);

/**
 * @brief  Destroy vector and free memory.
 * @param  v: pointer returned by vector_init.
 */
void vector_destroy(struct vector *v);

/**
 * @brief  Current number of stored elements.
 */
static inline size_t vector_size(const struct vector *v)
{
	return v->size;
}

/**
 * @brief  Capacity (allocated slots).
 */
static inline size_t vector_capacity(const struct vector *v)
{
	return v->capacity;
}

/**
 * @brief  Random-access (no bounds check).
 *
 * @param  v: vector pointer.
 * @param  idx: element index.
 *
 * @return void* pointer to element memory.
 */
static inline void *vector_at(struct vector *v, size_t idx)
{
	return (char *)v->data + (idx * v->elem_size);
}

/**
 * @brief  Append one element (by copy).
 *
 * @param  v:    vector pointer.
 * @param  elem: pointer to source object of elem_size bytes.
 *
 * @return 0 on success, -1 on realloc failure.
 */
int vector_push_back(struct vector *v, const void *elem);

/**
 * @brief  Remove last element (if any).
 *
 * @param  v: vector pointer.
 */
void vector_pop_back(struct vector *v);

/**
 * @brief  Reserve at least new_cap slots.
 *
 * @param  v:       vector pointer.
 * @param  new_cap: minimum capacity requested.
 *
 * @return 0 on success, -1 on realloc failure.
 */
int vector_reserve(struct vector *v, size_t new_cap);

/**
 * @brief  Clear contents (keep capacity).
 * @param  v: vector pointer.
 */
static inline void vector_clear(struct vector *v)
{
	v->size = 0;
}

#endif /* VECTOR_H */

