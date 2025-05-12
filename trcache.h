#ifndef TRCACHE_H
#define TRCACHE_H
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * @file	trcache.h
 * @brief	Public interface for the **trcache** library.
 *
 * The library buffers per-trade raw data, aggregates multiple candle types
 * (time-, tick- and month/week/day-based) and flushes them to storage after
 * a configurable threshold.  All functions are *thread-safe* unless otherwise
 * stated.
 */

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/*
 * @def   TRCACHE_SIMD_ALIGN
 * @brief Alignment (in bytes) of every vector array.
 *
 * 64 is enough for AVX-512 and current ARM SVE256. Increase if a wider
 * vector ISA comes along.
 */
#ifndef TRCACHE_SIMD_ALIGN
#define TRCACHE_SIMD_ALIGN (64)
#endif /* TRCACHE_SIMD_ALIGN */

typedef struct trcache trcache;

/*
 * trcache_trade_data - The basic unit provided by the user to trcache.
 *
 * @timestamp: Unix timestamp in milliseconds.
 * @trade_id:  Trade ID used to construct an n-tick candle.
 * @price:     Traded price of a single trade.
 * @volume:    Traded volume of a single trade.
 *
 * The address of this data structure is passed by the user as an argument to
 * the trcache_feed_trade_data(). Since the function does not deallocate this
 * structure internally, the user can declare it on their own stack before
 * calling the function.
 *
 * The raw data is reflected in all types of candles managed by trcache.
 * @timestamp is used to distinguish time-based candles, while @trade_id is used
 * to distinguish tick-based candles.
 */
typedef struct trcache_trade_data {
	uint64_t timestamp;
	uint64_t trade_id;
	double price;
	double volume;
} trcache_trade_data;

/*
 * Identifiers used by the user and trcache to recognize candle types.
 */
typedef enum {
	TRCACHE_MONTH_CANDLE    = 1 << 0,
	TRCACHE_WEEK_CANDLE     = 1 << 1,
	TRCACHE_DAY_CANDLE      = 1 << 2,
	TRCACHE_1H_CANDLE       = 1 << 3,
	TRCACHE_30MIN_CANDLE    = 1 << 4,
	TRCACHE_15MIN_CANDLE    = 1 << 5,
	TRCACHE_5MIN_CANDLE     = 1 << 6,
	TRCACHE_1MIN_CANDLE     = 1 << 7,
	TRCACHE_1SEC_CANDLE     = 1 << 8,
	TRCACHE_100TICK_CANDLE  = 1 << 9,
	TRCACHE_50TICK_CANDLE   = 1 << 10,
	TRCACHE_10TICK_CANDLE   = 1 << 11,
	TRCACHE_5TICK_CANDLE    = 1 << 12,
} trcache_candle_type;

#define TRCACHE_NUM_CANDLE_TYPE	(13)

typedef uint32_t trcache_candle_type_flags;

/*
 * Identifires for candle's fields. This is used in a bitmap to distinguish
 * fields, so the values should be a power of two.
 */
typedef enum {
	TRCACHE_FIRST_TIMESTAMP     = 1 << 0,
	TRCACHE_FIRST_TRADE_ID      = 1 << 1,
	TRCACHE_TIMESTAMP_INTERVAL  = 1 << 2,
	TRCACHE_TRADE_ID_INTERVAL   = 1 << 3,
	TRCACHE_OPEN                = 1 << 4,
	TRCACHE_HIGH                = 1 << 5,
	TRCACHE_LOW                 = 1 << 6,
	TRCACHE_CLOSE               = 1 << 7,
	TRCACHE_VOLUME              = 1 << 8,
} trcache_candle_field_type;

typedef uint32_t trcache_candle_field_flags;

/*
 * trcache_candle - Single candle data structured in row-oriented format.
 *
 * @first_timestamp:    Unix epoch in milliseconds of the first trade.
 * @first_trade_id:     Trade-ID of the first trade in the candle.
 * @timestamp_interval: Time interval of the candle in milliseconds.
 * @trade_id_interval:  Number of trades that make up the candle.
 * @open:               Price of the very first trade.
 * @high:               Highest traded price inside the candle.
 * @low:                Lowest traded price inside the candle.
 * @close:              Price of the very last trade.
 * @volume:             Sum of traded volume.
 */
typedef struct trcache_candle {
	uint64_t first_timestamp;
	uint64_t first_trade_id;
	uint32_t timestamp_interval;
	uint32_t trade_id_interval;
	double open;
	double high;
	double low;
	double close;
	double volume;
} trcache_candle;

/*
 * trcache_candle_batch - Vectorised batch of candles in column-oriented layout.
 *
 * @{field}_array: Vector arrays (all #TRCACHE_SIMD_ALIGN-aligned).
 * @num_candles:   Number of candles stored in every array.
 * @candle_type:   Engine-defined enum identifying timeframe / n-tick size.
 * @symbol_id:     Integer symbol ID resolved via the symbol table.
 *
 * All array members point into **one contiguous, SIMD-aligned block** so the
 * whole batch can be freed with a single 'free()' (or just unwound from the
 * stack if allocated by 'alloca').  Every array has exactly @num_candles
 * elements and shares the same element order.
 *
 * **Alignment note** – The engine guarantees that the starting address of the
 * block and each array pointer is aligned to 'TRCACHE_SIMD_ALIGN' bytes to
 * maximise SIMD load/store efficiency.
 */
typedef struct trcache_candle_batch {
	uint64_t *first_timestamp_array;
	uint64_t *first_trade_id_array;
	uint32_t *timestamp_interval_array;
	uint32_t *trade_id_interval_array;
	double *open_array;
	double *high_array;
	double *low_array;
	double *close_array;
	double *volume_array;
	int num_candles;
	int candle_type;
	int symbol_id;
} trcache_candle_batch;

/*
 * trcache_flush_ops - User-defined batch flush operation callbacks
 *
 * @flush:              User-defined batch flush function.
 * @is_done:            Checks whether the asynchronous flush has completed.
 * @destroy_handle:     Cleans up resources associated with the async handle.
 * @flush_ctx:          User‑supplied pointer, passed into @flush()
 * @destroy_handle_ctx: User-supplied pointer, passed into @destroy_handle().
 *
 * This structure lets applications plug in either synchronous or asynchronous
 * flush logic without changing the core engine.
 *
 * The flush worker calls @flush exactly once for every batch that reaches the
 * fully converted state. User's implementation has two options:
 *
 *   1. **Synchronous flush** – Perform the entire operation inside @flush and
 *      return **NULL**. The engine will treat the batch as flushed
 *      immediately and will not call @is_done or @destroy_handle.
 *
 *   2. **Asynchronous flush** – Initiate the operation inside @flush and return
 *      **a non‑NULL handle** (any unique pointer or token). The worker will
 *      keep that handle and periodically call @is_done until it returns true.
 *      After completion the worker will call @destroy_handle (if it is not
 *      NULL) to free any resources associated with the handle.
 */
typedef struct trcache_flush_ops {
	void *(*flush)(trcache *cache, trcache_candle_batch *batch, void *ctx);
	bool (*is_done)(trcache *cache, trcache_candle_batch *batch, void *handle);
	void (*destroy_handle)(void *handle, void *destroy_handle_ctx);
	void *flush_ctx;
	void *destroy_handle_ctx;
} trcache_flush_ops;

/*
 * trcache_init_ctx - All parameters required to create a *trcache*.
 *
 * @num_worker_threads:       Number of worker threads.
 * @batch_candle_count:       Fixed number of candles per column batch
 * @flush_threshold_batches:  How many candle batches to buffer before flush.
 * @candle_type_flags:        OR-ed set of #trcache_candle_type values.
 * @flush_ops:                User-supplied callbacks used for flush.
 *
 * Putting every knob in a single structure keeps the public API compact and
 * makes it forward-compatible (new members can be appended without changing the
 * `trcache_init()` signature).
 */
typedef struct trcache_init_ctx {
	int num_worker_threads;
	int batch_candle_count;
	int flush_threshold_batches;
	trcache_candle_type_flags candle_type_flags;
	struct trcache_flush_ops flush_ops;
} trcache_init_ctx;

/**
 * @brief Allocate and initialize the top-level trcache.
 *
 * @param ctx: Pointer to a fully-initialised #trcache_init_ctx.
 *
 * @return Pointer to trcache or NULL on failure.
 */
struct trcache *trcache_init(const struct trcache_init_ctx *ctx);

/**
 * @brief Destroy all trcache state, including per-thread caches.
 *
 * Safe to call after all worker threads have exited.
 *
 * @param cache: Handle from trcache_init().
 */
void trcache_destroy(struct trcache *cache);

/**
 * @brief   Register a new symbol string or return the existing ID.
 *
 * Thread-safe: uses internal hash map + mutex once per new symbol.
 *
 * @param	cache:		Handle from trcache_init().
 * @param	symbol_str:	NULL-terminated symbol string.
 *
 * @return	Symbol-ID ≥ 0 on success, −1 on failure.
 */
int trcache_register_symbol(struct trcache *cache, const char *symbol_str);

/**
 * @brief   Lookup symbol string by its symbol id.
 *
 * @param   cache:      Handle from trcache_init().
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 *
 * @return  NULL-terminated symbol string.
 */
const char *trcache_lookup_symbol_str(struct trcache *cache, int symbol_id);

/**
 * @brief   Push a single trade into the internal pipeline.
 *
 * @param	cache:			Handle from trcache_init().
 * @param	trade_data:		User-filled struct (copied internally).
 * @param	symbol_id:		ID obtained via trcache_register_symbol().
 */
void trcache_feed_trade_data(struct trcache *cache,
	struct trcache_trade_data *trade_data, int symbol_id);

/**
 * @brief  Allocate a contiguous, SIMD-aligned candle batch on the heap.
 *
 * @param num_candles: Number of OHLCV rows to allocate (must be > 0).
 *
 * @return Pointer to a fully-initialised #trcache_candle_batch on success,  
 *         'NULL' on allocation failure or invalid *num_candles*.
 *
 * @note The returned pointer must be released via trcache_batch_free().
 */
struct trcache_candle_batch *trcache_batch_alloc_on_heap(int num_candles);

/**
 * @brief  Release a heap-allocated candle batch.
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 *
 * @param batch: Pointer obtained from trcache_batch_alloc_on_heap().
 */
void trcache_batch_free(struct trcache_candle_batch *batch);

/**
 * @brief  Align a pointer upward to the next @p a-byte boundary.
 *
 * @param p: Raw pointer to be aligned.
 * @param a: Alignment in bytes (power-of-two, e.g. 64).
 *
 * @return Pointer guaranteed to satisfy ((uintptr_t)ret % a) == 0.
 *
 * @warning Macro clients must ensure @p p lies in a buffer of
 *          at least (a-1) extra bytes to avoid overflow.
 */
static inline void *trcache_align_up_ptr(void *p, size_t a)
{
	return (void *)(((uintptr_t)p + a - 1) & ~(uintptr_t)(a - 1));
}

/**
 * @brief  Build a fully-aligned candle batch on the *caller's stack*.
 *
 * Uses alloca() to reserve raw space for each array, then fixes
 * alignment via #trc_align_up_ptr.  The stack memory lives as long as the
 * caller's frame is active—no explicit free is required.
 *
 * @param dst [out]:   Pre-declared #trcache_candle_batch object to populate.
 * @param num_candles: Number of candle rows to allocate (must be > 0).
 *
 * @note All pointers inside @p dst point into the caller's stack frame.
 */
static inline void trcache_batch_alloc_on_stack(
	struct trcache_candle_batch *dst, int num_candles)
{
	const size_t a = TRCACHE_SIMD_ALIGN;
	size_t u64b = (size_t)num_candles * sizeof(uint64_t);
	size_t u32b = (size_t)num_candles * sizeof(uint32_t);
	size_t dblb = (size_t)num_candles * sizeof(double);

	uint8_t *buf_fts = alloca(u64b + a - 1);
	uint8_t *buf_ftid = alloca(u64b + a - 1);
	uint8_t *buf_ts_itv = alloca(u32b + a - 1);
	uint8_t *buf_tid_itv = alloca(u32b + a - 1);
	uint8_t *buf_op = alloca(dblb + a - 1);
	uint8_t *buf_hi = alloca(dblb + a - 1);
	uint8_t *buf_lo = alloca(dblb + a - 1);
	uint8_t *buf_cl = alloca(dblb + a - 1);
	uint8_t *buf_vol = alloca(dblb + a - 1);

	dst->num_candles = num_candles;
	dst->first_timestamp_array = (uint64_t *)trcache_align_up_ptr(buf_fts, a);
	dst->first_trade_id_array = (uint64_t *)trcache_align_up_ptr(buf_ftid, a);
	dst->timestamp_interval_array
		= (uint32_t *)trcache_align_up_ptr(buf_ts_itv, a);
	dst->trade_id_interval_array
		= (uint32_t *)trcache_align_up_ptr(buf_tid_itv, a);
	dst->open_array = (double *)trcache_align_up_ptr(buf_op, a);
	dst->high_array = (double *)trcache_align_up_ptr(buf_hi, a);
	dst->low_array = (double *)trcache_align_up_ptr(buf_lo, a);
	dst->close_array = (double *)trcache_align_up_ptr(buf_cl, a);
	dst->volume_array = (double *)trcache_align_up_ptr(buf_vol, a);
}

/**
 * @brief  Declare and initialise a stack-resident candle batch in one line.
 *
 * Example:
 * 
 * func() {
 *     TRCACHE_DEFINE_BATCH_ON_STACK(batch, 1024);
 *     // batch.open_array … 1024 aligned doubles
 *     ...
 * }
 *
 * @param var: User-chosen variable name of type #trcache_candle_batch.
 * @param num: Number of candles to allocate (runtime value allowed).
 */
#define TRCACHE_DEFINE_BATCH_ON_STACK(var, num) \
	trcache_candle_batch var; \
	trcache_batch_alloc_on_stack(&(var), (num))

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
