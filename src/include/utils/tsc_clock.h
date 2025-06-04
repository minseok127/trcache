#ifndef TSC_CLOCK_H
#define TSC_CLOCK_H

#include <stdint.h>
#include <time.h>
#include <unistd.h>

/**
 * @brief   Fetch the current 64‑bit cycle counter value.
 *
 * @return  Monotonically increasing cycle count (raw hardware counter).
 */
static inline uint64_t tsc_cycles(void)
{
	unsigned hi, lo;
	__asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
	return ((uint64_t)hi << 32) | lo;
}

/**
 * @brief   Obtain the counter frequency in cycles per second.
 *
 * Performs a lazy, one‑off calibration the first time it is called. Uses an
 * approximate 10 ms sleep to measure elapsed cycles against real time. The
 * result is cached in a process‑local static variable and returned on all
 * subsequent calls without locking.
 *
 * @return  Cycles per second (double precision).
 */
static inline double tsc_cycles_per_sec(void)
{
	static double cached_hz = 0.0;

	if (__builtin_expect(cached_hz == 0.0, 0)) {
		struct timespec ts0, ts1, req = {0, 10 * 1000 * 1000}; /* 10 ms */
		uint64_t c0, c1, ns;

		clock_gettime(CLOCK_MONOTONIC_RAW, &ts0);
		c0 = tsc_cycles();
		nanosleep(&req, NULL);
		clock_gettime(CLOCK_MONOTONIC_RAW, &ts1);
		c1 = tsc_cycles();

		ns = (uint64_t)(ts1.tv_sec - ts0.tv_sec) * 1000000000ull +
			(uint64_t)(ts1.tv_nsec - ts0.tv_nsec);
		cached_hz = (double)(c1 - c0) * 1e9 / (double)ns;
	}

	return cached_hz;
}

/**
 * @brief   Convert a cycle delta to nanoseconds using the calibrated frequency.
 *
 * @param   cycles: Cycle delta to convert.
 *
 * @return  Corresponding nanoseconds.
 */
static inline uint64_t tsc_cycles_to_ns(uint64_t cycles)
{
	double cps = tsc_cycles_per_sec();
	return (uint64_t)((double)cycles * (1e9 / cps));
}

#endif /* TSC_CLOCK_H */
