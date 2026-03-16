#ifndef TRCACHE_MEMORY_COMPAT_H
#define TRCACHE_MEMORY_COMPAT_H

/**
 * @file    compat/memory_compat.h
 * @brief   Aligned allocation / free compatibility layer.
 *
 * MSVC's _aligned_malloc has reversed argument order and requires
 * _aligned_free (plain free() crashes). These wrappers unify both.
 */

#include <stddef.h>

#ifdef _WIN32
#include <malloc.h>

static inline void *trc_aligned_alloc(size_t align, size_t size)
{
	return _aligned_malloc(size, align);
}

static inline void trc_aligned_free(void *ptr)
{
	_aligned_free(ptr);
}

#else /* POSIX */
#include <stdlib.h>

static inline void *trc_aligned_alloc(size_t align, size_t size)
{
	return aligned_alloc(align, size);
}

static inline void trc_aligned_free(void *ptr)
{
	free(ptr);
}

#endif /* _WIN32 */

#endif /* TRCACHE_MEMORY_COMPAT_H */
