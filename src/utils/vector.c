/**
 * @file   vector.c
 * @brief  Single-thread dynamic array (vector) implementation.
 *
 * Memory order is irrelevant because the container is intended for
 * single-thread use; no atomics are employed.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils/vector.h"
#include "utils/log.h"

struct vector *vector_init(size_t elem_size)
{
	struct vector *v = NULL;

	if (elem_size == 0) {
		errmsg(stderr, "Invalid argument (elem_size is 0)\n");
		return NULL;
	}

	v = malloc(sizeof(struct vector));

	if (v == NULL) {
		errmsg(stderr, "#vector allocation failed\n");
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
	void *tmp;

	if (new_cap <= v->capacity) {
		return 0;
	}

	tmp = realloc(v->data, new_cap * v->elem_size);

	if (!tmp) {
		errmsg(stderr, "Failure on realloc()\n");
		return -1;
	}

	v->data = tmp;
	v->capacity = new_cap;

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

