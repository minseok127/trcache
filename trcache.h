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
#define MAX_CANDLE_TYPES (32)

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
 * trcache_value - A flexible value type for trade data.
 *
 * This union allows trade price and volume to be represented as a double,
 * a 64-bit integer, or a generic pointer for custom types (e.g., decimal
 * libraries).
 */
typedef union {
	double as_double;
	int64_t as_int64;
	void *as_ptr;
} trcache_value;

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
	trcache_value price;
	trcache_value volume;
} trcache_trade_data;

/*
 * trcache_candle_base - Base structure for all user-defined candles.
 *
 * Every custom candle structure defined by the user must include this
 * structure as its very first member. This allows the trcache library to
 * safely access essential fields like the key and closed status while
 * remaining agnostic to the user-specific fields that follow.
 *
 * @key:       Union holding the unique identifier for the candle.
 * @is_closed: Non-zero when the candle has closed.
 */
typedef struct trcache_candle_base {
	union {
		uint64_t timestamp;
		uint64_t trade_id;
		uint64_t value;
	} key;
	bool is_closed;
} trcache_candle_base;

/*
 * trcache_candle_batch - Vectorised batch of candles in column-oriented layout.
 * 
 * This is a hybrid structure. Essential base fields (key, is_closed) are
 * exposed as named members for performance and convenience. Additional
 * user-defined fields are stored in the generic column_arrays.
 *
 * @key_array:     Array for the candle's unique key (always present).
 * @is_closed_array: Array for the candle's closed status (always present).
 * @column_arrays: Array of pointers to the actual columnar data arrays.
 * @capacity:      Capacity of vector arrays.
 * @num_candles:   Number of candles stored in every array.
 * @candle_idx:    The candle type identifier (index into config array).
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
	uint64_t *key_array;
	bool *is_closed_array;
	void **column_arrays;
	int capacity;
	int num_candles;
	int candle_idx;
	int symbol_id;
} trcache_candle_batch;

/*
 * trcache_batch_flush_ops - User-defined batch flush operation callbacks.
 *
 * @flush:                    User-defined batch flush function.
 * @is_done:                  Checks whether the asynchronous flush has completed.
 * @destroy_async_handle:     Cleans up resources associated with the async handle.
 * @on_batch_destroy:         Callback invoked just before a batch's memory is
 *                            freed, allowing the user to release custom
 *                            resources (e.g., pointers stored in candle
 *                            fields).
 * @flush_ctx:                User‑supplied pointer, passed into @flush().
 * @destroy_async_handle_ctx: User-supplied pointer, passed into
 *                            @destroy_async_handle().
 * @on_destroy_ctx:           User-supplied pointer, passed into
 *                            @on_batch_destroy().
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
 *      After completion the worker will call @destroy_handle (if
 */
typedef struct trcache_batch_flush_ops {
	void *(*flush)(trcache *cache, trcache_candle_batch *batch,
		void *flush_ctx);
	bool (*is_done)(trcache *cache, trcache_candle_batch *batch,
		void *async_handle);
	void (*destroy_async_handle)(void *async_handle,
		void *destroy_async_handle_ctx);
	void (*on_batch_destroy)(trcache_candle_batch *batch,
		void *on_destroy_ctx);
	void *flush_ctx;
	void *destroy_async_handle_ctx;
	void *on_destroy_ctx;
} trcache_batch_flush_ops;

/*
 * trcache_candle_update_ops - Callbacks for updating candles.
 *
 * @init:    Initialise a new candle using the first trade. This is
 *           invoked exactly once per candle before any calls to update().
 * @update:  Update an existing candle with a trade. The function must
 *           return true if the trade was consumed by this candle and false
 *           if the candle is already complete and the trade belongs in
 *           the subsequent candle.
 */
typedef struct trcache_candle_update_ops {
	void (*init)(struct trcache_candle_base *c, struct trcache_trade_data *d);
	bool (*update)(struct trcache_candle_base *c, struct trcache_trade_data *d);
} trcache_candle_update_ops;

/*
 * trcache_field_type - Enumerates the possible data types for a user-defined
 * candle field. This provides metadata for future library
 * features like internal analytics or serialization.
 */
typedef enum {
	FIELD_TYPE_UINT64,
	FIELD_TYPE_INT64,
	FIELD_TYPE_DOUBLE,
	FIELD_TYPE_UINT32,
	FIELD_TYPE_BOOL,
	FIELD_TYPE_POINTER
} trcache_field_type;

/*
 * trcache_field_def - Describes a single field within a user-defined candle
 *                     structure.
 *
 * An array of these structures acts as the schema for the custom candle,
 * allowing trcache to dynamically perform operations like the Convert stage.
 *
 * @offset:     The byte offset of the field within the custom structure
 *              (calculated using offsetof()).
 * @size:       The size of the field in bytes (calculated using sizeof()).
 * @type:       The data type of the field, from the trcache_field_type enum.
 */
typedef struct trcache_field_def {
	size_t offset;
	size_t size;
	trcache_field_type type;
} trcache_field_def;

/*
 * trcache_candle_config - Defines the properties and callbacks
 *                         for a single candle type.
 *
 * This structure encapsulates all user-provided information needed to manage
 * a specific candle type, including its closing condition, update logic,
 * and flush behavior.
 *
 * @user_candle_size:   The total size of the user-defined candle structure.
 * @field_definitions:  An array describing each field in the custom candle.
 * @num_fields:         The number of entries in the field_definitions array.
 * @update_ops:         Callbacks for initializing and updating a candle.
 * @flush_ops:          Callbacks for flushing a completed candle batch.
 */
typedef struct trcache_candle_config {
	size_t user_candle_size;
	const struct trcache_field_def *field_definitions;
	int num_fields;
	const struct trcache_candle_update_ops update_ops;
	const struct trcache_batch_flush_ops flush_ops;
} trcache_candle_config;

/*
 * trcache_field_request - Specifies which fields to retrieve in a query.
 *
 * @field_indices: An array of indices corresponding to the #trcache_field_def
 *                 array provided during initialization.
 * @num_fields:    The number of indices in the field_indices array.
 */
typedef struct trcache_field_request {
	const int *field_indices;
	int num_fields;
} trcache_field_request;

/*
 * trcache_init_ctx - All parameters required to create a *trcache*.
 *
 * @candle_configs:            Array of candle configurations.
 * @num_candle_configs:        Number of candle configurations provided.
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
	const struct trcache_candle_config *candle_configs;
	int num_candle_configs;
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
 * @brief   Copy @count candles ending at the candle with key @key.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
*                       Base fields are always included.
 * @param   key:        Key of the last candle to retrieve.
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_id_and_key(struct trcache *tc,
	int symbol_id, int candle_idx, const struct trcache_field_request *request,
	uint64_t key, int count, struct trcache_candle_batch *dst);

/**
 * @brief   Copy @count candles ending at the candle with key @key
 *          for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 *                      Base fields are always included.
 * @param   key:        Key of the last candle to retrieve.
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_key(struct trcache *tc,
	const char *symbol_str, int candle_idx,
	const struct trcache_field_request *request, uint64_t key, int count,
	struct trcache_candle_batch *dst);

/**
 * @brief   Copy @count candles ending at the candle located @offset from
 *          the most recent candle.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 *                      Base fields are always included.
 * @param   offset:     Offset from the most recent candle (0 == most recent).
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_id_and_offset(struct trcache *tc,
	int symbol_id, int candle_idx, const struct trcache_field_request *request,
	int offset, int count, struct trcache_candle_batch *dst);

/**
 * @brief   Copy @count candles ending at the candle located @offset from
 *          the most recent candle for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 *                      Base fields are always included.
 * @param   offset:     Offset from the most recent candle (0 == most recent).
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_offset(struct trcache *tc,
	const char *symbol_str, int candle_idx, 
	const struct trcache_field_request *request, int offset, int count,
	struct trcache_candle_batch *dst);

/**
 * @brief   Allocate a contiguous, SIMD-aligned candle batch on the heap.
 *
 * @param   tc:         Pointer to the trcache instance.
 * @param   candle_idx: Index of the candle configuration to use for layout.
 * @param   capacity:   Number of candle rows to allocate (must be > 0).
 * @param   request:    Pointer to a struct specifying which fields to allocate.
 *                      If NULL, all fields defined at init are allocated.
 *                      Base fields are always included.
 *
 * @return  Pointer to a fully-initialised #trcache_candle_batch on success,
 *          'NULL' on allocation failure or invalid *capacity*.
 *
 * @note    The returned pointer must be released via trcache_batch_free().
 */
struct trcache_candle_batch *trcache_batch_alloc_on_heap(struct trcache *tc,
	int candle_idx, int capacity, const struct trcache_field_request *request);

/**
 * @brief   Release a heap-allocated candle batch.
 *
 * This function is intended for batches allocated by the user via
 * trcache_batch_alloc_on_heap(). It only frees the memory block.
 * It does *not* invoke the on_batch_destroy() callback, which is reserved
 * for the internal pipeline. The user is responsible for cleaning up any
 * custom resources within the batch before calling this function.
 *
 * @param   batch: Pointer to the batch to be freed.
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 */
void trcache_batch_free(struct trcache_candle_batch *batch);

/**
 * Identifiers for pipeline stages executed by workers.
 */
typedef enum worker_stat_stage_type {
	WORKER_STAT_STAGE_APPLY = 0,
	WORKER_STAT_STAGE_CONVERT,
	WORKER_STAT_STAGE_FLUSH,
	WORKER_STAT_STAGE_NUM
} worker_stat_stage_type;

/**
 * Identifiers for memory usage categories tracked by memstat.
 */
typedef enum memstat_category {
	MEMSTAT_TRADE_DATA_BUFFER = 0,
	MEMSTAT_CANDLE_CHUNK_LIST,
	MEMSTAT_CANDLE_CHUNK_INDEX,
	MEMSTAT_SCQ_NODE,
	MEMSTAT_SCHED_MSG,
	MEMSTAT_CATEGORY_NUM
} memstat_category;

/**
 * trcache_worker_distribution_stats - Statistics on worker distribution
 *                                     and performance.
 *
 * @stage_speeds:     Measured average processing *efficiency per worker*
 *                    for each stage (items/sec/worker). Indicates how many
 *                    items a single worker processes per second on average.
 * @pipeline_demand:  Estimated *total required throughput* for each pipeline
 *                    stage across the entire system (items/sec).
 * @stage_capacity    Estimated *total processing capacity* for each stage
 *                    (items/sec). Represents the theoretical maximum throughput
 *                    if all allocated workers operate at peak efficiency.
 * @stage_limits:     Number of workers allocated to each stage,
 *                    indexed by 'worker_stat_stage_type'.
 * @stage_starts:     Starting worker index for each stage,
 *                    indexed by 'worker_stat_stage_type'.
 */
typedef struct trcache_worker_distribution_stats {
	double stage_speeds[WORKER_STAT_STAGE_NUM];
	double stage_capacity[WORKER_STAT_STAGE_NUM];
	double pipeline_demand[WORKER_STAT_STAGE_NUM];
	int stage_limits[WORKER_STAT_STAGE_NUM];
	int stage_starts[WORKER_STAT_STAGE_NUM];
} trcache_worker_distribution_stats;

/**
 * trcache_memory_stats - A snapshot of memory usage by category.
 *
 * @usage_bytes: An array holding the memory usage in bytes for each category,
 *               indexed by 'memstat_category'.
 */
typedef struct trcache_memory_stats {
	size_t usage_bytes[MEMSTAT_CATEGORY_NUM];
} trcache_memory_stats;

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
 * @brief   Get the current worker distribution and scheduler statistics.
 *
 * @param   cache:  Handle from trcache_init().
 * @param   stats:  Pointer to a user-allocated struct to be filled.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_worker_distribution(struct trcache *cache,
	trcache_worker_distribution_stats *stats);

/**
 * @brief   Get a snapshot of the auxiliary memory usage.
 *
 * @param   cache:  Handle from trcache_init().
 * @param   stats:  Pointer to a user-allocated struct to be filled.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_aux_memory_breakdown(struct trcache *cache,
	trcache_memory_stats *stats);

/**
 * @brief   Get a snapshot of the total memory usage.
 *
 * @param   cache:  Handle from trcache_init().
 * @param   stats:  Pointer to a user-allocated struct to be filled.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_total_memory_breakdown(struct trcache *cache,
	trcache_memory_stats *stats);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
