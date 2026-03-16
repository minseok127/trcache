#ifndef TRCACHE_BUILTIN_COMPAT_H
#define TRCACHE_BUILTIN_COMPAT_H

/**
 * @file    compat/builtin_compat.h
 * @brief   GCC built-in -> MSVC intrinsic compatibility layer.
 *
 * Provides platform-neutral wrappers for:
 *   __builtin_ctzll    -> trc_ctzll
 *   __builtin_expect   -> trc_expect
 *   __sync_synchronize -> trc_memory_barrier
 *   asm pause          -> trc_cpu_pause
 *   __uint128_t mul    -> trc_mul64_div64
 */

#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  trc_ctzll — count trailing zeros (64-bit)                         */
/* ------------------------------------------------------------------ */

#ifdef _MSC_VER
#include <intrin.h>

static inline int trc_ctzll(uint64_t x)
{
	unsigned long idx;
	_BitScanForward64(&idx, x);
	return (int)idx;
}

#else  /* !_MSC_VER */

static inline int trc_ctzll(uint64_t x)
{
	return __builtin_ctzll(x);
}

#endif /* _MSC_VER */

/* ------------------------------------------------------------------ */
/*  trc_expect — branch prediction hint                               */
/* ------------------------------------------------------------------ */

#ifdef _MSC_VER
#define trc_expect(expr, val) (expr)
#else  /* !_MSC_VER */
#define trc_expect(expr, val) __builtin_expect((expr), (val))
#endif /* _MSC_VER */

/* ------------------------------------------------------------------ */
/*  trc_memory_barrier — full memory fence                            */
/* ------------------------------------------------------------------ */

#ifdef _MSC_VER

static inline void trc_memory_barrier(void)
{
	MemoryBarrier();
}

#else  /* !_MSC_VER */

static inline void trc_memory_barrier(void)
{
	__sync_synchronize();
}

#endif /* _MSC_VER */

/* ------------------------------------------------------------------ */
/*  trc_cpu_pause — yield to hyper-thread sibling                     */
/* ------------------------------------------------------------------ */

#ifdef _MSC_VER
#include <immintrin.h>

static inline void trc_cpu_pause(void)
{
	_mm_pause();
}

#else  /* !_MSC_VER */

static inline void trc_cpu_pause(void)
{
	__asm__ __volatile__("pause");
}

#endif /* _MSC_VER */

/* ------------------------------------------------------------------ */
/*  trc_mul64_div64 — overflow-safe (a * b) / c for uint64_t          */
/*                                                                    */
/*  Replaces __uint128_t arithmetic on MSVC.                          */
/*  Identity: (a*b)/c == (a/c)*b + ((a%c)*b)/c                       */
/*  This is exact when (a%c)*b < 2^64, which holds for the           */
/*  trcache use case (b == 1e9, c == dt_ns ~ 1e8..1e10).             */
/* ------------------------------------------------------------------ */

static inline uint64_t trc_mul64_div64(uint64_t a, uint64_t b,
	uint64_t c)
{
	if (c == 0) {
		return 0;
	}
#ifdef _MSC_VER
	return (a / c) * b + ((a % c) * b) / c;
#else  /* !_MSC_VER */
	__uint128_t tmp = (__uint128_t)a * b;
	return (uint64_t)(tmp / c);
#endif /* _MSC_VER */
}

#endif /* TRCACHE_BUILTIN_COMPAT_H */
