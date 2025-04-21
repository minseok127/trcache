#ifndef TRCACHE_H
#define TRCACHE_H
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include <stdbool.h>
#include <stdint.h>

typedef struct trcache trcache;

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
 * An argument of the trcache_init().
 * @tick_candle_unit_list: tick-based candles to manage.
 * @time_candle_{}_unit_list: time-based candles to manage.
 * @num_{}_candles_units: size of the corresponding unit list.
 * @use_month_candle: whether to manage month-based candles.
 * @use_week_candle: whether to manage week-based candles.
 * @use_day_candle: whether to manage day-based candles.
 * @num_worker_threads: number of worker threads to be used by trcache.
 */
typedef struct trcache_init_context {
	int *tick_candle_unit_list;
	int *time_candle_hour_unit_list;
	int *time_candle_minute_unit_list;
	int *time_candle_second_unit_list;

	int num_tick_candle_units;
	int num_time_candle_hour_units;
	int num_time_candle_minute_units;
	int num_time_candle_second_units;

	bool use_month_candle;
	bool use_week_candle;
	bool use_day_candle;

	int num_worker_threads;
} trcache_init_context;

/* Initialize trcache structure */
struct trcache *trcache_init(struct trcache_init_context *context);

/* Destory trcache structure */
void trcache_destroy(struct trcache *cache);

/* Reflects a single trading data into the cache */
void trcache_add_raw_data(struct trcache *cache,
	struct trcache_raw_data *raw_data);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* TRCACHE_H */
