#ifndef TRCACHE_H
#define TRCACHE_H
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct trcache trcache;

#ifndef TRCACHE_SIMD_ALIGN
#define TRCACHE_SIMD_ALIGN (64)
#endif /* TRCACHE_SIMD_ALIGN */

/*
 * The basic unit provided by the user to trcache.
 * @timestamp: unix timestamp in milliseconds.
 * @trade_id: trade ID used to construct an n-tick candle.
 * @price: traded price of a single trade.
 * @volume: traded volume of a single trade.
 * @symbol_id: ID of the symbol to which the data will be applied.
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
typedef struct trcache_raw_data {
	uint64_t timestamp;
	uint64_t trade_id;
	double price;
	double volume;
	int symbol_id;
} trcache_raw_data;

/*
 * Identifiers used by the user and trcache to recognize candle types.
 */
typedef enum {
	TRCACHE_MONTH_CANDLE,
	TRCACHE_WEEK_CANDLE,
	TRCACHE_DAY_CANDLE,
	TRCACHE_1H_CANDLE,
	TRCACHE_30MIN_CANDLE,
	TRCACHE_15MIN_CANDLE,
	TRCACHE_5MIN_CANDLE,
	TRCACHE_1MIN_CANDLE,
	TRCACHE_1S_CANDLE,
	TRCACHE_100TICK_CANDLE,
	TRCACHE_50TICK_CANDLE,
	TRCACHE_10TICK_CANDLE,
	TRCACHE_5TICK_CANDLE,
} trcache_candle_type;

/*
 * A single candle data structured in row-oriented format.
 */
typedef struct trcache_candle {
	int symbol_id;
	int candle_type;
	uint64_t first_timestamp;
	uint64_t last_timestamp;
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
	uint64_t *last_timestamp_array;
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
 * An argument of the trcache_init().
 * @use_{}_candle: flags to specify which candles trcache should manage.
 * @num_worker_threads: number of worker threads to be used by trcache.
 * @total_num_candles: how many candles the cache will keep in memory.
 */
typedef struct trcache_init_context {
	bool use_month_candle;
	bool use_week_candle;
	bool use_day_candle;

	bool use_1h_candle;
	bool use_30min_candle;
	bool use_15min_candle;
	bool use_5min_candle;
	bool use_1min_candle;
	bool use_1s_candle;

	bool use_100tick_candle;
	bool use_50tick_candle;
	bool use_10tick_candle;
	bool use_5tick_candle;

	int num_worker_threads;
	int total_num_candles;
} trcache_init_context;

/* Initialize trcache structure */
struct trcache *trcache_init(struct trcache_init_context *context);

/* Destory trcache structure */
void trcache_destroy(struct trcache *cache);

/* Reflects a single trading data into the cache */
void trcache_add_raw_data(struct trcache *cache,
	struct trcache_raw_data *raw_data);

/* Allocates an entire candle batch in heap memory */
struct trcache_candle_batch *trcache_heap_alloc_candle_batch(int capacity);

/* Allocates a selective candle batch in heap memory */
struct trcache_candle_batch *trcache_heap_alloc_candle_batch_selective(
	int capacity, bool use_first_timestamp, bool use_last_timestamp,
	bool use_open, bool use_high, bool use_low, bool use_close,
	bool use_volume);

/* Frees the candle batch from heap memory */
void trcache_heap_free_candle_batch(struct trcache_candle_batch *batch);

/* Allocates an entire candle batch in stack memory */
void trcache_stack_alloc_candle_batch(struct trcache_candle_batch *b, int c);

/* Allocates a selective candle batch in stack memory */
void trcache_stack_alloc_candle_batch_selective(struct trcache_candle_batch *b,
	int capacity, bool use_first_timestamp, bool use_last_timestamp,
	bool use_open, bool use_high, bool use_low, bool use_close,
	bool use_volume);

#define TRCACHE_DEFINE_CANDLE_BATCH_ON_STACK(var, capacity) \
	struct trcache_candle_batch var; \
	trcache_stack_alloc_candle_batch(&(var), (capacity));

#define TRCACHE_DEFINE_SELECTIVE_CANDLE_BATCH_ON_STACK(var, capacity, \
	use_first_timestamp, use_last_timestamp, use_open, use_high, use_low, \
	use_close, use_volume) \
	struct trcache_candle_batch var; \
	trcache_stack_alloc_candle_batch(&(var), (capacity), \
		(use_first_timestamp), (use_last_timestamp), (use_open), (use_high), \
		(use_low), (use_close), (use_volume));

/* Exports old candles in trcache into the given batch argument */
void trcache_export_candle_batch(struct trcache *cache,
	struct trcache_candle_batch *exported_batch, 
	int symbol_id, int candle_type);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
