/*
 * @file    candle_update_ops.c
 * @brief   Implementation of struct candle_update_ops callbacks.
 * 
 * This module manages all type of candle_update_ops callback functions and
 * gives the pointer of the callback functions to the other modules by
 * get_candle_update_ops() function.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pipeline/candle_update_ops.h"
#include "utils/log.h"

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define MIN(a,b) ((a) < (b) ? (a) : (b))

#define SEC(x)      ((uint64_t)(x) * 1000ULL)
#define MINUTE(x)   SEC((x) * 60)
#define HOUR(x)     MINUTE((x) * 60)
#define DAY(x)      HOUR((x) * 24)

/* Time-interval candles */
#define DEFINE_TIME_CANDLE_OPS(SUFFIX, INTERVAL_MS)                      \
static void init_##SUFFIX(struct trcache_candle *c,                      \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	uint64_t start = d->timestamp - (d->timestamp % (INTERVAL_MS));      \
	c->start_timestamp = start;                                          \
	c->start_trade_id = d->trade_id;                                     \
	c->open = c->high = c->low = c->close = d->price;                    \
	c->volume = d->volume;                                               \
}                                                                        \
static void update_##SUFFIX(struct trcache_candle *c,                    \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	c->high = MAX(c->high, d->price);                                    \
	c->low = MIN(c->low, d->price);                                      \
	c->close = d->price;                                                 \
	c->volume += d->volume;                                              \
}                                                                        \
static bool is_closed_##SUFFIX(struct trcache_candle *c,                 \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	return (d->timestamp / (INTERVAL_MS))                                \
		!= (c->start_timestamp / (INTERVAL_MS));                         \
}                                                                        \
static const struct candle_update_ops ops_##SUFFIX = {                   \
	.init = init_##SUFFIX,                                               \
	.update = update_##SUFFIX,                                           \
	.is_closed = is_closed_##SUFFIX,                                     \
}

/* Tick-count candles */
#define DEFINE_TICK_CANDLE_OPS(SUFFIX, INTERVAL_TICK)                    \
static void init_##SUFFIX(struct trcache_candle *c,                      \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	uint64_t base = d->trade_id - (d->trade_id % (INTERVAL_TICK));       \
	c->start_trade_id = base;                                            \
	c->start_timestamp = d->timestamp;                                   \
	c->open = c->high = c->low = c->close = d->price;                    \
	c->volume = d->volume;                                               \
}                                                                        \
static void update_##SUFFIX(struct trcache_candle *c,                    \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	c->high = MAX(c->high, d->price);                                    \
	c->low = MIN(c->low, d->price);                                      \
	c->close = d->price;                                                 \
	c->volume += d->volume;                                              \
}                                                                        \
static bool is_closed_##SUFFIX(struct trcache_candle *c,                 \
	struct trcache_trade_data *d)                                        \
{                                                                        \
	return (d->trade_id / (INTERVAL_TICK))                               \
		!= (c->start_trade_id / (INTERVAL_TICK));                        \
}                                                                        \
static const struct candle_update_ops ops_##SUFFIX = {                   \
	.init = init_##SUFFIX,                                               \
	.update = update_##SUFFIX,                                           \
	.is_closed = is_closed_##SUFFIX,                                     \
}

/* Day, Hour, Minute, Second (ms) */
DEFINE_TIME_CANDLE_OPS(day,    DAY(1));
DEFINE_TIME_CANDLE_OPS(1h,     HOUR(1));
DEFINE_TIME_CANDLE_OPS(30min,  MINUTE(30));
DEFINE_TIME_CANDLE_OPS(15min,  MINUTE(15));
DEFINE_TIME_CANDLE_OPS(5min,   MINUTE(5));
DEFINE_TIME_CANDLE_OPS(1min,   MINUTE(1));
DEFINE_TIME_CANDLE_OPS(1sec,   SEC(1));

/* Tick candles */
DEFINE_TICK_CANDLE_OPS(100tick, 100);
DEFINE_TICK_CANDLE_OPS(50tick,   50);
DEFINE_TICK_CANDLE_OPS(10tick,   10);
DEFINE_TICK_CANDLE_OPS(5tick,     5);

static const struct candle_update_ops ops_month = {
	.init = NULL,
	.update = NULL,
	.is_closed = NULL,
};

static const struct candle_update_ops ops_week = {
	.init = NULL,
	.update = NULL,
	.is_closed = NULL,
};

/* Index == bit position in trcache_candle_type */
static const struct candle_update_ops *const ops_tbl[TRCACHE_NUM_CANDLE_TYPE] =
{
	&ops_month,
	&ops_week,
	&ops_day,
	&ops_1h,
	&ops_30min,
	&ops_15min,
	&ops_5min,
	&ops_1min,
	&ops_1sec,
	&ops_100tick,
	&ops_50tick,
	&ops_10tick,
	&ops_5tick
};

/**
 * @brief   Return update operations for a given candle type.
 *
 * @param   type: Candle type identifier (must represent a single type).
 *
 * @return  Pointer to the corresponding candle_update_ops.
 */
const struct candle_update_ops *get_candle_update_ops(trcache_candle_type type)
{
	unsigned idx;

	/* Accept only single-bit values */
	if (!type || (type & (type - 1))) {
		errmsg(stderr, "Invalid candle type\n");
		return NULL;
	}

	idx = (unsigned)__builtin_ctz((unsigned)type);
	if (idx >= TRCACHE_NUM_CANDLE_TYPE) {
		return NULL;
	}

	return ops_tbl[idx];
}
