#ifndef TRCACHE_H
#define TRCACHE_H
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct trcache trcache;

/*
 * trcache_trade_data - the basic unit provided by the user to trcache.
 *
 * @timestamp:	unix timestamp in milliseconds.
 * @trade_id:	trade ID used to construct an n-tick candle.
 * @price:		traded price of a single trade.
 * @volume:		traded volume of a single trade.
 * @symbol_id:	ID of the symbol to which the data will be applied.
 *
 * The address of this data structure is passed by the user as an argument to
 * the trcache_add_raw_data(). Since the function does not deallocate this
 * structure internally, the user can declare it on their own stack before
 * calling the function.
 *
 * The raw data is reflected in all types of candles managed by trcache.
 * @timestamp is used to distinguish time-based candles, while @trade_id is used
 * to distinguish tick-based candles.
 *
 * The symbol ID is an integer value used by the user to distinguish between
 * different trading symbols. In other words, how actual trading targets are
 * distinguished is not determined by trcache but should be defined by the
 * user's own logic.
 */
typedef struct trcache_trade_data {
	uint64_t timestamp;
	uint64_t trade_id;
	double price;
	double volume;
	int symbol_id;
} trcache_trade_data;

/*
 * Identifiers used by the user and trcache to recognize candle types.
 */
typedef enum {
	TRCACHE_MONTH_CANDLE = 1 << 0,
	TRCACHE_WEEK_CANDLE = 1 << 1,
	TRCACHE_DAY_CANDLE = 1 << 2,
	TRCACHE_1H_CANDLE = 1 << 3,
	TRCACHE_30MIN_CANDLE = 1 << 4,
	TRCACHE_15MIN_CANDLE = 1 << 5,
	TRCACHE_5MIN_CANDLE = 1 << 6,
	TRCACHE_1MIN_CANDLE = 1 << 7,
	TRCACHE_1S_CANDLE = 1 << 8,
	TRCACHE_100TICK_CANDLE = 1 << 9,
	TRCACHE_50TICK_CANDLE = 1 << 10,
	TRCACHE_10TICK_CANDLE = 1 << 11,
	TRCACHE_5TICK_CANDLE = 1 << 12,
} trcache_candle_type;

typedef uint32_t trcache_candle_type_flags;

/*
 * Identifires for candle's fields. This is used in a bitmap to distinguish
 * fields, so the values should be a power of two.
 */
typedef enum {
	TRCACHE_FIRST_TIMESTAMP = 1 << 0,
	TRCACHE_FIRST_TRADE_ID = 1 << 1,
	TRCACHE_TIMESTAMP_INTERVAL = 1 << 2,
	TRCACHE_TRADE_ID_INTERVAL = 1 << 3,
	TRCACHE_OPEN = 1 << 4,
	TRCACHE_HIGH = 1 << 5,
	TRCACHE_LOW = 1 << 6,
	TRCACHE_CLOSE = 1 << 7,
	TRCACHE_VOLUME = 1 << 8,
} trcache_candle_field_type;

typedef uint32_t trcache_candle_field_flags;

/*
 * A single candle data structured in row-oriented format.
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
 * A collection of multiple candles structured in column-oriented format.
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
	int capacity;
	int num_candles;
	int candle_type;
	int symbol_id;
} trcache_candle_batch;

/* 
 * trcache_init - allocate and initialize the top-level trcache.
 *
 * @num_worker_threads: number of threads that will feed data
 * @flush_threshold:    how many items to buffer before flush
 * @candle_type_flags:  which data types to track
 *
 * Returns pointer or NULL on failure.
 */
struct trcache *trcache_init(int num_worker_threads, int flush_threshold_candles,
	trcache_candle_type_flags candle_type_flags);

/*
 * Destroy all trcache state, including per-thread caches.
 * Safe to call after all worker threads have exited.
 */
void trcache_destroy(struct trcache *cache);

/*
 * Register or lookup a symbol string in the shared symbol table.
 * Thread-safe, may be called concurrently.
 * Returns symbol ID >= 0 on success, or -1 on error.
 */
int trcache_register_symbol(struct trcache *cache, const char *symbol_str);

/*
 * Feed a single trading raw data into the trcache.
 * Caller must fill a trcache_trade_data struct.
 */
void trcache_feed_trade_data(struct trcache *cache,
	struct trcache_trade_data *trade_data);

/* Allocates an entire candle batch in heap memory */
struct trcache_candle_batch *trcache_heap_alloc_candle_batch(int capacity);

/* Allocates a selective candle batch in heap memory */
struct trcache_candle_batch *trcache_heap_alloc_candle_batch_selective(
	int capacity, trcache_candle_field_flags field_flag);

/* Frees the candle batch from heap memory */
void trcache_heap_free_candle_batch(struct trcache_candle_batch *batch);

/* Allocates an entire candle batch in stack memory */
void trcache_stack_alloc_candle_batch(struct trcache_candle_batch *b, int c);

/* Allocates a selective candle batch in stack memory */
void trcache_stack_alloc_candle_batch_selective(struct trcache_candle_batch *b,
	int capacity, trcache_candle_field_flags field_flag);

#define TRCACHE_DEFINE_CANDLE_BATCH_ON_STACK(var, capacity) \
	struct trcache_candle_batch var; \
	trcache_stack_alloc_candle_batch(&(var), (capacity));

#define TRCACHE_DEFINE_SELECTIVE_CANDLE_BATCH_ON_STACK(var, capacity, flag) \
	struct trcache_candle_batch var; \
	trcache_stack_alloc_candle_batch(&(var), (capacity), (flag));

/* Exports old candles in trcache into the given batch argument */
void trcache_export_candle_batch(struct trcache *cache,
	struct trcache_candle_batch *exported_batch, 
	int symbol_id, enum trcache_candle_type candle_type);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
