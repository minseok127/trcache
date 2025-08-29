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
 * a configurable threshold. All functions are *thread-safe* unless otherwise
 * stated.
 */

#include <alloca.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define MAX_NUM_THREADS (1024)
#define MAX_CANDLE_TYPES_PER_BASE (32)

/*
 * @brief   Alignment (in bytes) of every vector array.
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
 * the trcache_feed_trade_data(). Since the function copies this trade data
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
 * Identifiers for candle's fields. This is used in a bitmap to distinguish
 * fields, so the values should be a power of two.
 */
typedef enum {
	TRCACHE_START_TIMESTAMP     = 1 << 0,
	TRCACHE_OPEN                = 1 << 1,
	TRCACHE_HIGH                = 1 << 2,
	TRCACHE_LOW                 = 1 << 3,
	TRCACHE_CLOSE               = 1 << 4,
	TRCACHE_VOLUME              = 1 << 5,
	TRCACHE_TRADING_VALUE       = 1 << 6,
	TRCACHE_TRADE_COUNT         = 1 << 7,
	TRCACHE_IS_CLOSED           = 1 << 8
} trcache_candle_field_type;

#define TRCACHE_NUM_CANDLE_FIELD (9)

typedef uint32_t trcache_candle_field_flags;

#define TRCACHE_FIELD_MASK_ALL \
	(((trcache_candle_field_flags)1 << TRCACHE_NUM_CANDLE_FIELD) - 1)

/*
 * This enum serves as a high-level classifier for different candle aggregation
 * strategies. For example, CANDLE_TIME_BASE groups all candles formed based on
 * time intervals, while CANDLE_TICK_BASE groups those formed by a fixed
 * number of trades. This design allows for the flexible addition of entirely
 * new candle formation concepts in the future.
 */
typedef enum {
	CANDLE_TIME_BASE,
	CANDLE_TICK_BASE,
	NUM_CANDLE_BASES
} trcache_candle_base;

/**
 * trcache_candle_type - Uniquely identifies a specific candle type.
 *
 * This structure is used for all communication between the user and the trcache
 * system regarding candle types. It specifies both the fundamental base
 * (e.g., time-based) and the specific instance within that base (e.g., the
 * second time-based candle type, which might be a 5-minute candle).
 *
 * @base:      The fundamental category of the candle (e.g., CANDLE_TIME_BASE).
 * @type_idx:  A zero-based index identifying the specific candle type within
 *             the given base.
 */
typedef struct {
	trcache_candle_base base;
	int type_idx;
} trcache_candle_type;

/*
 * trcache_candle - Single candle data structured in row-oriented format.
 *
 * @start_timestamp:    Start timestamp covered by the candle
 *                      (unix epoch in milliseconds).
 * @open:               Price of the very first trade.
 * @high:               Highest traded price inside the candle.
 * @low:                Lowest traded price inside the candle.
 * @close:              Price of the very last trade.
 * @volume:             Sum of traded volume.
 * @trading_value:      Sum of each trade's price multiplied by its volume.
 * @trade_count:        Number of trades aggregated into this candle.
 * @is_closed:          Non-zero when the candle has closed.
 */
typedef struct trcache_candle {
	uint64_t start_timestamp;
	double open;
	double high;
	double low;
	double close;
	double volume;
	double trading_value;
	uint32_t trade_count;
	bool is_closed;
	uint8_t pad[3];
} trcache_candle;

/*
 * trcache_candle_batch - Vectorised batch of candles in column-oriented layout.
 *
 * @{field}_array: Vector arrays (all #TRCACHE_SIMD_ALIGN-aligned).
 * @capacity:      Capacity of vector arrays.
 * @num_candles:   Number of candles stored in every array.
 * @candle_type:   The candle type identifier.
 * @symbol_id:     Integer symbol ID resolved via symbol table.
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
	uint64_t *start_timestamp_array;
	double *open_array;
	double *high_array;
	double *low_array;
	double *close_array;
	double *volume_array;
	double *trading_value_array;
	uint32_t *trade_count_array;
	bool *is_closed_array;
	int capacity;
	int num_candles;
	trcache_candle_type candle_type;
	int symbol_id;
} trcache_candle_batch;

/*
 * trcache_flush_ops - User-defined batch flush operation callbacks
 *
 * @flush:              User-defined batch flush function.
 * @is_done:            Checks whether the asynchronous flush has completed.
 * @destroy_handle:     Cleans up resources associated with the async handle.
 * @flush_ctx:          User‑supplied pointer, passed into @flush().
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
	void *(*flush)(trcache *cache, trcache_candle_batch *batch, void *flush_ctx);
	bool (*is_done)(trcache *cache, trcache_candle_batch *batch, void *handle);
	void (*destroy_handle)(void *handle, void *destroy_handle_ctx);
	void *flush_ctx;
	void *destroy_handle_ctx;
} trcache_flush_ops;

/*
 * candle_update_ops - Callbacks for updating candles.
 *
 * @init:    Initialise a new candle using the first trade. This is
 *           invoked exactly once per candle before any calls to update().
 * @update:  Update an existing candle with a trade. The function must
 *           return true if the trade was consumed by this candle and false
 *           if the candle is already complete and the trade belongs in
 *           the subsequent candle.
 */
typedef struct candle_update_ops {
	void (*init)(struct trcache_candle *c, struct trcache_trade_data *d);
	bool (*update)(struct trcache_candle *c, struct trcache_trade_data *d);
} candle_update_ops;

/* Helper macros for time-interval candles */
#define DEFINE_TIME_CANDLE_OPS(SUFFIX, INTERVAL_MS)                      \
static void init_##SUFFIX(struct trcache_candle *c,                      \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	uint64_t start = d->timestamp - (d->timestamp % (INTERVAL_MS));      \
	c->start_timestamp = start;                                          \
	c->open = c->high = c->low = c->close = d->price;                    \
	c->volume = d->volume;                                               \
	c->trading_value = d->price * d->volume;                             \
	c->trade_count = 1;                                                  \
	c->is_closed = false;                                                \
}                                                                        \
static bool update_##SUFFIX(struct trcache_candle *c,                    \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	if ((d->timestamp / (INTERVAL_MS))                                   \
			!= (c->start_timestamp / (INTERVAL_MS))) {                   \
		c->is_closed = true;                                             \
		return false;                                                    \
	}                                                                    \
	c->high = c->high > d->price ? c->high : d->price;                   \
	c->low = c->low < d->price ? c->low : d->price;                      \
	c->close = d->price;                                                 \
	c->volume += d->volume;                                              \
	c->trading_value += d->price * d->volume;                            \
	c->trade_count += 1;                                                 \
	return true;                                                         \
}                                                                        \
static const struct candle_update_ops ops_##SUFFIX = {                   \
	.init = init_##SUFFIX,                                               \
	.update = update_##SUFFIX,                                           \
}

/* Helper macro for tick-count candles */
#define DEFINE_TICK_CANDLE_OPS(SUFFIX, INTERVAL_TICK)                    \
static void init_##SUFFIX(struct trcache_candle *c,                      \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	uint64_t rem = d->trade_id % (INTERVAL_TICK);                        \
	c->start_timestamp = d->timestamp;                                   \
	c->open = c->high = c->low = c->close = d->price;                    \
	c->volume = d->volume;                                               \
	c->trading_value = d->price * d->volume;                             \
	c->trade_count = (uint32_t)rem + 1;                                  \
	c->is_closed = false;                                                \
}                                                                        \
static void update_##SUFFIX(struct trcache_candle *c,                    \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	if (c->trade_count >= (INTERVAL_TICK)) {                             \
		c->is_closed = true;                                             \
		return false;                                                    \
	}                                                                    \
	c->high = c->high > d->price ? c->high : d->price;                   \
	c->low = c->low < d->price ? c->low : d->price;                      \
	c->close = d->price;                                                 \
	c->volume += d->volume;                                              \
	c->trading_value += d->price * d->volume;                            \
	c->trade_count += 1;                                                 \
	return true;                                                         \
}                                                                        \
static const struct candle_update_ops ops_##SUFFIX = {                   \
	.init = init_##SUFFIX,                                               \
	.update = update_##SUFFIX,                                           \
}

/**
 * trcache_candle_config - Defines the properties and callbacks
 *                         for a single candle type.
 *
 * This structure encapsulates all user-provided information needed to manage
 * a specific candle type, including its closing condition, update logic,
 * and flush behavior.
 *
 * @threshold:  A union holding the threshold value for closing a candle.
 * @update_ops: Callbacks for initializing and updating a candle.
 * @flush_ops:  Callbacks for flushing a completed candle batch.
 */
typedef struct {
	union {
		uint64_t interval_ms; /* For CANDLE_TIME_BASE */
		uint32_t num_ticks;   /* For CANDLE_TICK_BASE */
	} threshold;
	const struct candle_update_ops update_ops;
	const struct trcache_flush_ops flush_ops;
} trcache_candle_config;

/*
 * trcache_init_ctx - All parameters required to create a *trcache*.
 *
 * @candle_types:              Array of pointers to candle configuration arrays.
 * @num_candle_types:          Number of candle types for each candle base.
 * @batch_candle_count_pow2:   Number of candles per column batch(log2(cap)).
 * @cached_batch_count_pow2:   Number of batches to cache (log2(cap)).
 * @aux_memory_limit:          Maximum number of bytes this trcache may use
 *                             for auxiliary data structures (i.e. everything
 *                             other than candle chunk list/index).
 * @num_worker_threads:        Number of worker threads.
 *
 * Putting every knob in a single structure keeps the public API compact and
 * makes it forward-compatible (new members can be appended without changing the
 * 'trcache_init()' signature).
 */
typedef struct trcache_init_ctx {
	const trcache_candle_config *candle_types[NUM_CANDLE_BASES];
	int num_candle_types[NUM_CANDLE_BASES];
	int batch_candle_count_pow2;
	int cached_batch_count_pow2;
	size_t aux_memory_limit;
	int num_worker_threads;
} trcache_init_ctx;

/**
 * @brief   Allocate and initialize the top-level trcache.
 *
 * @param   ctx: Pointer to a fully-initialised #trcache_init_ctx.
 *
 * @return  Pointer to trcache or NULL on failure.
 */
struct trcache *trcache_init(const struct trcache_init_ctx *ctx);

/**
 * @brief   Destroy all trcache state, including per-thread caches.
 *
 * @param   cache: Handle from trcache_init().
 *
 * Safe to call after all worker threads have exited.
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
 * @brief   Lookup symbol ID by its symbol string.
 *
 * Uses a per-thread cache for fast lookups and falls back to the
 * shared symbol table if necessary.
 *
 * @param   cache:      Handle from trcache_init().
 * @param   symbol_str: NULL-terminated symbol string.
 *
 * @return  Symbol-ID on success or -1 if not found.
 */
int trcache_lookup_symbol_id(struct trcache *cache, const char *symbol_str);

/**
 * @brief   Push a single trade into the internal pipeline.
 *
 * @param   cache:       Handle from trcache_init().
 * @param   trade_data:  User-filled struct (copied internally).
 * @param   symbol_id:   ID obtained via trcache_register_symbol().
 *
 * @return  0 on success, -1 on error.
 *
 * XXX Currently, it is assumed that no more than one user thread receives trade
 * data for a given symbol. If multiple users push trade data for the same
 * symbol concurrently, the implementation must be modified accordingly.
 */
int trcache_feed_trade_data(struct trcache *cache,
	struct trcache_trade_data *trade_data, int symbol_id);

/**
 * @brief   Copy @count time‑based candles ending at @ts_end for a symbol ID.
 *
 * @param   cache:          Handle returned by trcache_init().
 * @param   symbol_id:      Symbol ID obtained from trcache_register_symbol().
 * @param   time_type_idx:  Zero‑based index into the time interval array
 *                          supplied via #trcache_init_ctx.
 * @param   field_mask:     Bitmask of desired candle fields.
 * @param   ts_end:         Timestamp belonging to the last candle (inclusive).
 * @param   count:          Number of candles to copy.
 * @param   batch:          Pre‑allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_time_candles_by_symbol_id_ts(trcache *cache,
	int symbol_id, int time_type_idx, trcache_candle_field_flags field_mask,
	uint64_t ts_end, int count, trcache_candle_batch *batch);

/**
 * @brief   Copy @count time‑based candles ending at @ts_end
 *          for a symbol string.
 *
 * @param   cache:          Handle returned by trcache_init().
 * @param   symbol_str:     NULL‑terminated symbol string.
 * @param   time_type_idx:  Zero‑based index into the time interval array
 *                          supplied via #trcache_init_ctx.
 * @param   field_mask:     Bitmask of desired candle fields.
 * @param   ts_end:         Timestamp belonging to the last candle (inclusive).
 * @param   count:          Number of candles to copy.
 * @param   batch:          Pre‑allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_time_candles_by_symbol_str_ts(trcache *cache,
	const char *symbol_str, int time_type_idx,
	trcache_candle_field_flags field_mask, uint64_t ts_end, int count,
	trcache_candle_batch *batch);

/**
 * @brief   Copy @count time‑based candles ending at the candle located
 *          @offset from the most recent candle for a symbol ID.
 *
 * @param   cache:          Handle returned by trcache_init().
 * @param   symbol_id:      Symbol ID obtained from trcache_register_symbol().
 * @param   time_type_idx:  Index into the time interval array supplied via
 *                          #trcache_init_ctx.
 * @param   field_mask:     Bitmask of desired candle fields.
 * @param   offset:         Offset from the most recent candle (offset 0).
 * @param   count:          Number of candles to copy.
 * @param   batch:          Pre‑allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_time_candles_by_symbol_id_offset(trcache *cache,
	int symbol_id, int time_type_idx, trcache_candle_field_flags field_mask,
	int offset, int count, trcache_candle_batch *batch);

/**
 * @brief   Copy @count time‑based candles ending at the candle located
 *          @offset from the most recent candle for a symbol string.
 *
 * @param   cache:          Handle returned by trcache_init().
 * @param   symbol_str:     NULL‑terminated symbol string.
 * @param   time_type_idx:  Index into the time interval array supplied via
 *                          #trcache_init_ctx.
 * @param   field_mask:     Bitmask of desired candle fields.
 * @param   offset:         Offset from the most recent candle (offset 0).
 * @param   count:          Number of candles to copy.
 * @param   batch:          Pre‑allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_time_candles_by_symbol_str_offset(trcache *cache,
	const char *symbol_str, int time_type_idx,
	trcache_candle_field_flags field_mask, int offset, int count,
	trcache_candle_batch *batch);

/**
 * @brief   Copy @count tick‑based candles ending at the candle located
 *          @offset from the most recent candle for a symbol ID.
 *
 * @param   cache:          Handle returned by trcache_init().
 * @param   symbol_id:      Symbol ID obtained from trcache_register_symbol().
 * @param   tick_type_idx:  Index into the tick interval array supplied via
 *                          #trcache_init_ctx.
 * @param   field_mask:     Bitmask of desired candle fields.
 * @param   offset:         Offset from the most recent candle (offset 0).
 * @param   count:          Number of candles to copy.
 * @param   batch:          Pre‑allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_tick_candles_by_symbol_id_offset(trcache *cache,
	int symbol_id, int tick_type_idx, trcache_candle_field_flags field_mask,
	int offset, int count, trcache_candle_batch *batch);

/**
 * @brief   Copy @count tick‑based candles ending at the candle located
 *          @offset from the most recent candle for a symbol string.
 *
 * @param   cache:          Handle returned by trcache_init().
 * @param   symbol_str:     NULL‑terminated symbol string.
 * @param   tick_type_idx:  Index into the tick interval array supplied via
 *                          #trcache_init_ctx.
 * @param   field_mask:     Bitmask of desired candle fields.
 * @param   offset:         Offset from the most recent candle (offset 0).
 * @param   count:          Number of candles to copy.
 * @param   batch:          Pre‑allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_tick_candles_by_symbol_str_offset(trcache *cache,
	const char *symbol_str, int tick_type_idx,
	trcache_candle_field_flags field_mask, int offset, int count,
	trcache_candle_batch *batch);

/**
 * @brief   Allocate a contiguous, SIMD-aligned candle batch on the heap.
 *
 * @param   capacity:   Number of OHLCV rows to allocate (must be > 0).
 * @param   field_mask: OR-ed set of required fields.
 *
 * @return  Pointer to a fully-initialised #trcache_candle_batch on success,
 *          'NULL' on allocation failure or invalid *capacity*.
 *
 * @note The returned pointer must be released via trcache_batch_free().
 */
struct trcache_candle_batch *trcache_batch_alloc_on_heap(int capacity,
	trcache_candle_field_flags field_mask);

/**
 * @brief   Release a heap-allocated candle batch.
 *
 * @param   batch: Pointer obtained from trcache_batch_alloc_on_heap().
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 */
void trcache_batch_free(struct trcache_candle_batch *batch);

/**
 * @brief   Print current worker distribution per pipeline stage.
 *
 * Updates pipeline statistics and computes the number of workers allocated
 * to each pipeline stage according to the admin scheduler.
 *
 * @param   cache:  Handle from trcache_init().
 */
void trcache_print_worker_distribution(struct trcache *cache);

/**
 * @brief   Print a breakdown of the auxiliary memory usage of a trcache.
 *
 * @param   cache: Pointer to a trcache instance as returned from trcache_init().
 */
void trcache_print_aux_memory_breakdown(struct trcache *cache);

/**
 * @brief   Print a breakdown of the total memory usage of a trcache.
 *
 * @param   cache: Pointer to a trcache instance as returned from trcache_init().
 */
void trcache_print_total_memory_breakdown(struct trcache *cache);

/**
 * @brief   Align a pointer upward to the next @p a-byte boundary.
 *
 * @param   p: Raw pointer to be aligned.
 * @param   a: Alignment in bytes (power-of-two, e.g. 64).
 *
 * @return  Pointer guaranteed to satisfy ((uintptr_t)ret % a) == 0.
 *
 * @warning Macro clients must ensure @p p lies in a buffer of
 *          at least (a-1) extra bytes to avoid overflow.
 */
static inline void *trcache_align_up_ptr(void *p, size_t a)
{
	return (void *)(((uintptr_t)p + a - 1) & ~(uintptr_t)(a - 1));
}

/**
 * @brief   Build a fully-aligned candle batch on the *caller's stack*.
 *
 * Uses alloca() to reserve raw space for each array, then fixes
 * alignment via #trc_align_up_ptr.  The stack memory lives as long as the
 * caller's frame is active—no explicit free is required.
 *
 * @param   dst [out]:  Pre-declared #trcache_candle_batch object to populate.
 * @param   capacity:   Number of candle rows to allocate (must be > 0).
 * @param   field_mask: OR-ed set of required fields.
 *
 * @note All pointers inside @p dst point into the caller's stack frame.
 */
static inline void trcache_batch_alloc_on_stack(
	struct trcache_candle_batch *dst, int capacity,
	trcache_candle_field_flags field_mask)
{
	const size_t a = TRCACHE_SIMD_ALIGN;
	size_t u64b = (size_t)capacity * sizeof(uint64_t);
	size_t dblb = (size_t)capacity * sizeof(double);
	size_t u32b = (size_t)capacity * sizeof(uint32_t);
	size_t boolb = (size_t)capacity * sizeof(bool);

	uint8_t *buf_ts = NULL;
	uint8_t *buf_op = NULL;
	uint8_t *buf_hi = NULL;
	uint8_t *buf_lo = NULL;
	uint8_t *buf_cl = NULL;
	uint8_t *buf_vol = NULL;
	uint8_t *buf_tv = NULL;
	uint8_t *buf_tc = NULL;
	uint8_t *buf_ic = NULL;

	if (field_mask & TRCACHE_START_TIMESTAMP)
		buf_ts = alloca(u64b + a - 1);
	if (field_mask & TRCACHE_OPEN)
		buf_op = alloca(dblb + a - 1);
	if (field_mask & TRCACHE_HIGH)
		buf_hi = alloca(dblb + a - 1);
	if (field_mask & TRCACHE_LOW)
		buf_lo = alloca(dblb + a - 1);
	if (field_mask & TRCACHE_CLOSE)
		buf_cl = alloca(dblb + a - 1);
	if (field_mask & TRCACHE_VOLUME)
		buf_vol = alloca(dblb + a - 1);
	if (field_mask & TRCACHE_TRADING_VALUE)
		buf_tv = alloca(dblb + a - 1);
	if (field_mask & TRCACHE_TRADE_COUNT)
		buf_tc = alloca(u32b + a - 1);
	if (field_mask & TRCACHE_IS_CLOSED)
		buf_ic = alloca(boolb + a - 1);

	dst->capacity = capacity;
	dst->num_candles = 0;
	dst->symbol_id = -1;

	dst->start_timestamp_array = (field_mask & TRCACHE_START_TIMESTAMP) ?
		(uint64_t *)trcache_align_up_ptr(buf_ts, a) : NULL;
	dst->open_array = (field_mask & TRCACHE_OPEN) ?
		(double *)trcache_align_up_ptr(buf_op, a) : NULL;
	dst->high_array = (field_mask & TRCACHE_HIGH) ?
		(double *)trcache_align_up_ptr(buf_hi, a) : NULL;
	dst->low_array = (field_mask & TRCACHE_LOW) ?
		(double *)trcache_align_up_ptr(buf_lo, a) : NULL;
	dst->close_array = (field_mask & TRCACHE_CLOSE) ?
		(double *)trcache_align_up_ptr(buf_cl, a) : NULL;
	dst->volume_array = (field_mask & TRCACHE_VOLUME) ?
		(double *)trcache_align_up_ptr(buf_vol, a) : NULL;
	dst->trading_value_array = (field_mask & TRCACHE_TRADING_VALUE) ?
		(double *)trcache_align_up_ptr(buf_tv, a) : NULL;
	dst->trade_count_array = (field_mask & TRCACHE_TRADE_COUNT) ?
		(uint32_t *)trcache_align_up_ptr(buf_tc, a) : NULL;
	dst->is_closed_array = (field_mask & TRCACHE_IS_CLOSED) ?
		(bool *)trcache_align_up_ptr(buf_ic, a) : NULL;
}

/**
 * @brief   Declare and initialise a stack-resident candle batch in one line.
 *
 * Example:
 * 
 * func() {
 *     TRCACHE_DEFINE_BATCH_ON_STACK(batch, 1024, TRCACHE_FIELD_MASK_ALL);
 *     // batch.open_array … 1024 aligned doubles
 *     ...
 * }
 *
 * @param   var:  User-chosen variable name of type #trcache_candle_batch.
 * @param   cap:  Number of candles to allocate (runtime value allowed).
 * @param   mask: OR-ed set of required fields.
 */
#define TRCACHE_DEFINE_BATCH_ON_STACK(var, cap, mask) \
	trcache_candle_batch var; \
	trcache_batch_alloc_on_stack(&(var), (cap), (mask))

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
