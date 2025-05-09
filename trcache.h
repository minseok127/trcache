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

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

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
	TRCACHE_MONTH_CANDLE	= 1 << 0,
	TRCACHE_WEEK_CANDLE		= 1 << 1,
	TRCACHE_DAY_CANDLE		= 1 << 2,
	TRCACHE_1H_CANDLE		= 1 << 3,
	TRCACHE_30MIN_CANDLE	= 1 << 4,
	TRCACHE_15MIN_CANDLE	= 1 << 5,
	TRCACHE_5MIN_CANDLE		= 1 << 6,
	TRCACHE_1MIN_CANDLE		= 1 << 7,
	TRCACHE_1S_CANDLE		= 1 << 8,
	TRCACHE_100TICK_CANDLE	= 1 << 9,
	TRCACHE_50TICK_CANDLE	= 1 << 10,
	TRCACHE_10TICK_CANDLE	= 1 << 11,
	TRCACHE_5TICK_CANDLE	= 1 << 12,
} trcache_candle_type;

#define TRCACHE_NUM_CANDLE_TYPE	(13)

typedef uint32_t trcache_candle_type_flags;

/*
 * Identifires for candle's fields. This is used in a bitmap to distinguish
 * fields, so the values should be a power of two.
 */
typedef enum {
	TRCACHE_FIRST_TIMESTAMP		= 1 << 0,
	TRCACHE_FIRST_TRADE_ID		= 1 << 1,
	TRCACHE_TIMESTAMP_INTERVAL	= 1 << 2,
	TRCACHE_TRADE_ID_INTERVAL	= 1 << 3,
	TRCACHE_OPEN				= 1 << 4,
	TRCACHE_HIGH				= 1 << 5,
	TRCACHE_LOW					= 1 << 6,
	TRCACHE_CLOSE				= 1 << 7,
	TRCACHE_VOLUME				= 1 << 8,
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
	int num_candles;
	int candle_type;
	int symbol_id;
} trcache_candle_batch;

/* 
 * trcache_init - Allocate and initialize the top-level trcache.
 *
 * @param num_worker_threads: number of threads that will feed data
 * @param flush_threshold:    how many items to buffer before flush
 * @param candle_type_flags:  which data types to track
 *
 * @return Pointer to trcache or NULL on failure.
 */
struct trcache *trcache_init(int num_worker_threads, int flush_threshold_candles,
	trcache_candle_type_flags candle_type_flags);

/*
 * trcache_destroy - Destroy all trcache state, including per-thread caches.
 * Safe to call after all worker threads have exited.
 */
void trcache_destroy(struct trcache *cache);

/**
 * trcache_register_symbol()
 *
 * Register a new symbol string or return the existing ID.
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
 * trcache_feed_trade_data()
 *
 * Push a single trade into the internal pipeline.
 *
 * @param	cache:			Handle from trcache_init().
 * @param	trade_data:		User-filled struct (copied internally).
 * @param	symbol_id:		ID obtained via trcache_register_symbol().
 */
void trcache_feed_trade_data(struct trcache *cache,
	struct trcache_trade_data *trade_data, int symbol_id);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
