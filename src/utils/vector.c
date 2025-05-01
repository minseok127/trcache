/**
 * @file   vector.c
 * @brief  Single-thread dynamic array (vector) implementation.
 *
 * Memory order is irrelevant because the container is intended for
 * single-thread use; no atomics are employed.  Realloc growth policy is
 * power-of-two starting at 8 elements.
 */

#include "vector.h"

/**
 * @brief	Round x up to the next power-of-two (minimum 8).
 *
 * @param	x:	Value to round.
 * @return	Smallest power-of-two ≥ x and ≥ 8.
 *
 * @note	Bit-twiddling hack works for 32/64-bit size_t.
 */
static size_t next_pow2(size_t x)
{
	if (x < 8) {
		return 8;
	}

	x--;				/* make 111… pattern below current bit */
	x |= x >> 1;
	x |= x >> 2;
	x |= x >> 4;
#if SIZE_MAX > 0xFFFF
	x |= x >> 8;
#endif
#if SIZE_MAX > 0xFFFFFFFF
	x |= x >> 16;
	x |= x >> 32;
#endif
	return ++x;			/* carry into next power-of-two */
}


struct vector *vector_init(size_t elem_size)
{
	struct vector *v = NULL;

	if (elem_size == 0) {
		fprintf(stderr, "vector_init: invalid elem_size\n");
		return NULL;
	}

	v = malloc(sizeof(struct vector));

	if (v == NULL) {
		fprintf(stderr, "vector_init: malloc failed\n");
		return NULL;
	}

	v->data = NULL;
	v->elem_size = elem_size;
	v->size = 0;
	v->capacity = 0;

	return v;
}

void vector_destroy(struct vector *v)
{
	if (!v) {
		return;
	}

	if (v->data != NULL) {
		free(v->data);
	}

	free(v);
}

/**
 * @brief	Ensure capacity is at least @new_cap elements.
 *
 * @return	0 on success, −1 on realloc failure.
 */
int vector_reserve(struct vector *v, size_t new_cap)
{
	size_t cap;
	void *tmp;

	if (new_cap <= v->capacity) {
		return 0;
	}

	cap = next_pow2(new_cap);
	tmp = realloc(v->data, cap * v->elem_size);

	if (!tmp) {
		fprintf(stderr, "vector_reserve: realloc failed\n");
		return -1;
	}

	v->data = tmp;
	v->capacity = cap;

	return 0;
}

/**
 * @brief	Push one element to the back (by copy).
 *
 * @return	0 on success, −1 on realloc failure.
 */
int vector_push_back(struct vector *v, const void *elem)
{
	void *dst = NULL;

	if (!v || !elem) {
		return -1;
	}

	/* grow when full (double-capacity or start with 8) */
	if (v->size == v->capacity &&
	    vector_reserve(v, v->capacity ? v->capacity * 2 : 8) < 0) {
		return -1;
	}

	dst = (char *)v->data + v->size * v->elem_size;
	memcpy(dst, elem, v->elem_size);
	v->size += 1;

	return 0;
}

/**
 * @brief	Remove last element (if any).
 *
 * This simply decrements size; memory is kept for reuse.
 */
void vector_pop_back(struct vector *v)
{
	if (!v || v->size == 0) {
		return;
	}

	v->size -= 1;
}

