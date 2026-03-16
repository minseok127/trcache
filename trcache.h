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

#ifdef _WIN32
#include <malloc.h>   /* _alloca */
#define alloca _alloca
#else  /* !_WIN32 */
#include <alloca.h>
#endif /* _WIN32 */
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define MAX_NUM_THREADS (1024)
#define MAX_CANDLE_TYPES (32)

/*
 * 64 is enough for AVX-512 and current ARM SVE256.
 * Increase if a wider vector ISA comes along.
 */
#ifndef TRCACHE_SIMD_ALIGN
#define TRCACHE_SIMD_ALIGN (64)
#endif /* TRCACHE_SIMD_ALIGN */

/*
 * 64-byte cache line alignment.
 */
#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE (64)
#endif /* CACHE_LINE_SIZE */

/*
 * Data structure memory alignement macro.
 */
#ifndef ____cacheline_aligned
#ifdef _MSC_VER
#define ____cacheline_aligned __declspec(align(CACHE_LINE_SIZE))
#else  /* !_MSC_VER */
#define ____cacheline_aligned __attribute__((aligned(CACHE_LINE_SIZE)))
#endif /* _MSC_VER */
#endif /* ____cacheline_aligned */

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
 * trcache_trade_data - A default trade data structure.
 *
 * @timestamp: Unix timestamp in milliseconds.
 * @trade_id:  Trade ID used to construct an n-tick candle.
 * @price:     Traded price of a single trade.
 * @volume:    Traded volume of a single trade.
 *
 * NOTE: The trcache engine no longer enforces this specific structure. Users
 * can define their own trade data structures of any size. This definition
 * is kept for convenience and backward compatibility.
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
 * @key_array:       Array for the candle's unique key (always present).
 * @is_closed_array: Array for the candle's closed status (always present).
 * @column_arrays:   Array of pointers to the actual columnar data arrays.
 * @capacity:        Capacity of vector arrays.
 * @num_candles:     Number of candles stored in every array.
 * @candle_idx:      The candle type identifier (index into config array).
 * @symbol_id:       Integer symbol ID resolved via symbol table.
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
 * @flush:   Initiate a flush for one fully-converted candle batch.
 *           The implementation tracks async state internally via @ctx.
 * @is_done: Poll for flush completion. Returns true when done (success or
 *           error). Always called after @flush; never called before it.
 *           The implementation must clean up any internal async state before
 *           returning true.
 * @ctx:     User-supplied pointer passed to both @flush and @is_done.
 *
 * The flush worker calls @flush exactly once per fully-converted batch,
 * then polls @is_done until it returns true. Error reporting is the
 * implementation's responsibility (e.g. via a callback stored in @ctx).
 */
typedef struct trcache_batch_flush_ops {
	void (*flush)(trcache *cache, trcache_candle_batch *batch, void *ctx);
	bool (*is_done)(trcache *cache, trcache_candle_batch *batch, void *ctx);
	void *ctx;
} trcache_batch_flush_ops;

/*
 * trcache_trade_flush_ops - User-defined raw trade block flush callbacks.
 *
 * @flush:   Initiate a flush for one completed raw trade block.
 *           The implementation tracks async state internally via @ctx,
 *           keyed by @io_block (the block's unique buffer pointer).
 * @is_done: Poll for flush completion. Returns true when done (success
 *           or error). Always called after @flush; never called before
 *           it. The implementation must clean up any internal async
 *           state before returning true.
 * @ctx:     User-supplied pointer passed to both @flush and @is_done.
 *
 * Setting @flush to NULL disables raw trade persistence entirely.
 * Error reporting is the implementation's responsibility (e.g. via a
 * callback stored in @ctx).
 */
typedef struct trcache_trade_flush_ops {
	void (*flush)(trcache *cache, int symbol_id,
		const void *io_block, int num_trades,
		void *ctx);
	bool (*is_done)(trcache *cache,
		const void *io_block, void *ctx);
	void *ctx;
} trcache_trade_flush_ops;

/*
 * trcache_book_state_ops - User-defined book state management callbacks.
 *
 * @ctx:      User-supplied pointer passed to all callbacks.
 * @init:     Create and return a new book state for a symbol. Called
 *            lazily on the first book event, not at registration.
 * @update:   Update the book state with a single book event.
 * @destroy:  Free all resources owned by the book state.
 *
 * Setting @init to NULL disables the book pipeline entirely.
 */
typedef struct trcache_book_state_ops {
	void *ctx;
	void *(*init)(trcache *cache, int symbol_id,
		void *ctx);
	void (*update)(void *state, const void *event,
		void *ctx);
	void (*destroy)(void *state, void *ctx);
} trcache_book_state_ops;

/*
 * trcache_book_event_flush_ops - User-defined book event block flush
 *                                callbacks.
 *
 * @flush:   Initiate a flush for one completed book event block.
 *           Same async pattern as trcache_trade_flush_ops.
 * @is_done: Poll for flush completion. Returns true when done.
 * @ctx:     User-supplied pointer passed to both callbacks.
 *
 * Setting @flush to NULL disables book event persistence.
 */
typedef struct trcache_book_event_flush_ops {
	void (*flush)(trcache *cache, int symbol_id,
		const void *io_block, int num_events,
		void *ctx);
	bool (*is_done)(trcache *cache,
		const void *io_block, void *ctx);
	void *ctx;
} trcache_book_event_flush_ops;

/*
 * trcache_candle_update_ops - Callbacks for updating candles.
 *
 * @init:    Initialise a new candle using the first trade. This is
 *           invoked exactly once per candle before any calls to update().
 * @update:  Update an existing candle with a trade. The function must
 *           return true if the trade was consumed by this candle and false
 *           if the candle is already complete and the trade belongs in
 *           the subsequent candle.
 *
 * Both callbacks receive a pointer to the user-defined trade data structure.
 * Users must cast this 'void *' to their own structure type.
 */
typedef struct trcache_candle_update_ops {
	void (*init)(struct trcache_candle_base *c, void *trade_data,
		const void *book_state);
	bool (*update)(struct trcache_candle_base *c, void *trade_data,
		const void *book_state);
} trcache_candle_update_ops;

/*
 * trcache_field_type - Enumerates the possible data types for a 
 *                      user-defined candle field. 
 *
 * This provides metadata for future library features like internal analytics
 * or serialization.
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
 * @batch_flush_ops:    Callbacks for flushing a completed candle batch.
 */
typedef struct trcache_candle_config {
	size_t user_candle_size;
	const struct trcache_field_def *field_definitions;
	int num_fields;
	const struct trcache_candle_update_ops update_ops;
	const struct trcache_batch_flush_ops batch_flush_ops;
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
 * @total_memory_limit:        The total memory limit in bytes.
 * @num_worker_threads:        Number of worker threads.
 * @max_symbols:               Maximum number of symbols that can be registered.
 * @trade_data_size:           Size of the user-defined trade data structure.
 * @feed_block_size:           Target I/O block size for event data blocks
 *                             in bytes. Set to 0 to use the default (64 KiB).
 *                             Each block's data buffer is allocated with
 *                             4 KiB alignment.
 * @trade_flush_ops:           Optional callbacks to persist raw trade blocks.
 *                             Set .flush = NULL to disable.
 * @book_state_ops:            Optional book state management callbacks.
 *                             Set .init = NULL to disable.
 * @book_event_flush_ops:      Optional callbacks to persist book event
 *                             blocks. Set .flush = NULL to disable.
 * @book_event_size:           Size of the user-defined book event structure.
 *                             Required when book_state_ops.init != NULL.
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
	size_t total_memory_limit;
	int num_worker_threads;
	int max_symbols;
	size_t trade_data_size;
	size_t feed_block_size;
	struct trcache_trade_flush_ops trade_flush_ops;
	struct trcache_book_state_ops book_state_ops;
	struct trcache_book_event_flush_ops book_event_flush_ops;
	size_t book_event_size;
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
 * @param   trade_data:  Pointer to user-defined trade data (copied internally).
 * @param   symbol_id:   ID obtained via trcache_register_symbol().
 *
 * @return  0 on success, -1 on error.
 *
 * The function copies @trade_data_size bytes from the @trade_data pointer.
 *
 * XXX Currently, it is assumed that no more than one user thread receives trade
 * data for a given symbol. If multiple users push trade data for the same
 * symbol concurrently, the implementation must be modified accordingly.
 */
int trcache_feed_trade_data(struct trcache *cache, const void *trade_data,
	int symbol_id);

/**
 * @brief   Push a single book event into the internal pipeline.
 *
 * @param   cache:       Handle from trcache_init().
 * @param   book_event:  Pointer to user-defined book event (copied).
 * @param   symbol_id:   ID obtained via trcache_register_symbol().
 *
 * @return  0 on success, -1 on error.
 *
 * Requires book_state_ops.init != NULL (book pipeline enabled).
 */
int trcache_feed_book_data(struct trcache *cache,
	const void *book_event, int symbol_id);

/**
 * @brief   Copy @count candles ending at the candle with key @key.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 *                      Base fields are always included.
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
 * The user is responsible for cleaning up any custom resources within
 * the batch before calling this function.
 *
 * @param   batch: Pointer to the batch to be freed.
 *
 * Safe to pass 'NULL'; the function becomes a no-op.
 */
void trcache_batch_free(struct trcache_candle_batch *batch);

/**
 * @brief   Copy candles within the key range [start_key, end_key].
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 *                      Base fields are always included.
 * @param   start_key:  Key of the first candle (inclusive).
 * @param   end_key:    Key of the last candle (inclusive).
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure (e.g., capacity insufficient).
 *
 * @note    If start_key or end_key is outside the available range, the query
 *          is clamped to the available bounds. If no candles fall within the
 *          range, dst->num_candles is set to 0 and the function returns 0.
 *          Returns -1 if dst->capacity is insufficient to hold all candles
 *          in the range.
 */
int trcache_get_candles_by_symbol_id_and_key_range(struct trcache *tc,
	int symbol_id, int candle_idx,
	const struct trcache_field_request *request,
	uint64_t start_key, uint64_t end_key,
	struct trcache_candle_batch *dst);

/**
 * @brief   Copy candles within the key range [start_key, end_key]
 *          for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 *                      Base fields are always included.
 * @param   start_key:  Key of the first candle (inclusive).
 * @param   end_key:    Key of the last candle (inclusive).
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_key_range(struct trcache *tc,
	const char *symbol_str, int candle_idx,
	const struct trcache_field_request *request,
	uint64_t start_key, uint64_t end_key,
	struct trcache_candle_batch *dst);

/**
 * @brief   Count candles within the key range [start_key, end_key].
 *
 * Use this function to determine the required capacity before calling
 * trcache_get_candles_by_symbol_id_and_key_range().
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   start_key:  Key of the first candle (inclusive).
 * @param   end_key:    Key of the last candle (inclusive).
 *
 * @return  Number of candles in the range (>= 0), or -1 on failure.
 *
 * @note    If start_key or end_key is outside the available range, the count
 *          is based on the clamped bounds. Returns 0 if no candles exist
 *          in the range or the list is empty.
 */
int trcache_count_candles_by_symbol_id_and_key_range(struct trcache *tc,
	int symbol_id, int candle_idx,
	uint64_t start_key, uint64_t end_key);

/**
 * @brief   Count candles within the key range [start_key, end_key]
 *          for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   candle_idx: Candle type to query (index into config array).
 * @param   start_key:  Key of the first candle (inclusive).
 * @param   end_key:    Key of the last candle (inclusive).
 *
 * @return  Number of candles in the range (>= 0), or -1 on failure.
 */
int trcache_count_candles_by_symbol_str_and_key_range(struct trcache *tc,
	const char *symbol_str, int candle_idx,
	uint64_t start_key, uint64_t end_key);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
