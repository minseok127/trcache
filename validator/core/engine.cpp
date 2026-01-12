/*
 * validator/core/engine.cpp
 *
 * Implementation of the engine wrapper.
 * Uses a "Slot System" to map runtime configuration thresholds
 * to static C function pointers required by trcache.
 */

#include "engine.h"
#include "types.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <cmath>

/* --------------------------------------------------------------------------
 * Internal Constants & Globals
 * -------------------------------------------------------------------------- */

/* Maximum number of candle configurations supported dynamically */
#define MAX_CANDLE_SLOTS 8

/*
 * Global array to hold threshold values for each slot.
 * Accessed by static callback functions.
 */
static struct val_candle_config g_slot_configs[MAX_CANDLE_SLOTS];

/* --------------------------------------------------------------------------
 * 1. Callback Implementations (Logic)
 * -------------------------------------------------------------------------- */

/*
 * init_common - Shared initialization logic for all candle types.
 */
static void init_common(trcache_candle_base* c, trcache_trade_data* d,
			val_candle* candle)
{
	double price = d->price.as_double;

	/* OHLCV Setup */
	candle->open = price;
	candle->high = price;
	candle->low = price;
	candle->close = price;
	candle->volume = d->volume.as_double;

	/* Metrics */
	candle->exchange_ts = d->timestamp;
	candle->local_ts = 0;

	/* Integrity */
	candle->start_seq_id = d->trade_id;
	candle->end_seq_id = d->trade_id;

	candle->tick_count = 1;
	c->is_closed = false;
}

/*
 * update_common - Shared update logic (OHLCV + ID).
 */
static void update_common(val_candle* candle, trcache_trade_data* d)
{
	double price = d->price.as_double;

	if (price > candle->high) candle->high = price;
	if (price < candle->low)  candle->low = price;
	candle->close = price;
	candle->volume += d->volume.as_double;

	/* Update tracking info */
	candle->exchange_ts = d->timestamp;
	candle->end_seq_id = d->trade_id;
	candle->tick_count++;
}

/*
 * flush_noop - Memory-only flush (No disk I/O).
 * Returns NULL to indicate synchronous completion.
 */
static void* flush_noop(struct trcache* cache,
			struct trcache_candle_batch* batch,
			void* ctx)
{
	(void)cache;
	(void)batch;
	(void)ctx;
	return NULL;
}

/* --------------------------------------------------------------------------
 * 2. Slot Template System
 * --------------------------------------------------------------------------
 */

/*
 * Helper macros to define callback functions for a specific slot index.
 * We need distinct functions for each slot to use the correct threshold.
 */

/* TICK_MODULO Logic */
template <int SLOT>
void init_tick_modulo(trcache_candle_base* c, trcache_trade_data* d)
{
	val_candle* candle = (val_candle*)c;
	int threshold = g_slot_configs[SLOT].threshold;

	init_common(c, d, candle);
	/* Key Alignment: 3 -> 0 if threshold is 10 */
	c->key.trade_id = d->trade_id - (d->trade_id % threshold);
}

template <int SLOT>
bool update_tick_modulo(trcache_candle_base* c, trcache_trade_data* d)
{
	val_candle* candle = (val_candle*)c;
	int threshold = g_slot_configs[SLOT].threshold;

	/*
	 * [Safety Guard] Check if the trade belongs to this candle.
	 * If we have a gap (e.g. 8 -> 10), 10 is outside the [0, 10) range.
	 */
	if (d->trade_id >= c->key.trade_id + threshold) {
		/* Force close the partial candle */
		c->is_closed = true;
		
		/* Capture close time (Latency for partial candle) */
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		candle->local_ts = (uint64_t)ts.tv_sec * 1000000ULL +
				   (uint64_t)ts.tv_nsec / 1000;

		/* Return false: Do NOT consume this trade, use it for next */
		return false; 
	}

	update_common(candle, d);

	/* Close Condition: (ID + 1) % threshold == 0 */
	if ((d->trade_id + 1) % threshold == 0) {
		c->is_closed = true;
		/* Capture latency timestamp */
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		candle->local_ts = (uint64_t)ts.tv_sec * 1000000ULL +
				   (uint64_t)ts.tv_nsec / 1000;
	}
	return true; /* Include this trade */
}

/* TIME_FIXED Logic */
template <int SLOT>
void init_time_fixed(trcache_candle_base* c, trcache_trade_data* d)
{
	val_candle* candle = (val_candle*)c;
	int threshold = g_slot_configs[SLOT].threshold;

	init_common(c, d, candle);
	/* Key Alignment: Floor timestamp */
	c->key.timestamp = d->timestamp - (d->timestamp % threshold);
}

template <int SLOT>
bool update_time_fixed(trcache_candle_base* c, trcache_trade_data* d)
{
	val_candle* candle = (val_candle*)c;
	int threshold = g_slot_configs[SLOT].threshold;

	/* Check bounds first */
	if (d->timestamp >= c->key.timestamp + threshold) {
		c->is_closed = true;
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		candle->local_ts = (uint64_t)ts.tv_sec * 1000000ULL +
				   (uint64_t)ts.tv_nsec / 1000;
		return false; /* Trade belongs to next candle */
	}

	update_common(candle, d);
	return true; /* Include this trade */
}

/*
 * Function Pointer Arrays
 * These lookup tables map a runtime slot index to the compiled template instance.
 */
typedef void (*init_func_t)(trcache_candle_base*, trcache_trade_data*);
typedef bool (*update_func_t)(trcache_candle_base*, trcache_trade_data*);

/* Define instances for slots 0 to 7 */
static const init_func_t INIT_TICK_OPS[] = {
	init_tick_modulo<0>, init_tick_modulo<1>, init_tick_modulo<2>,
	init_tick_modulo<3>, init_tick_modulo<4>, init_tick_modulo<5>,
	init_tick_modulo<6>, init_tick_modulo<7>
};

static const update_func_t UPDATE_TICK_OPS[] = {
	update_tick_modulo<0>, update_tick_modulo<1>, update_tick_modulo<2>,
	update_tick_modulo<3>, update_tick_modulo<4>, update_tick_modulo<5>,
	update_tick_modulo<6>, update_tick_modulo<7>
};

static const init_func_t INIT_TIME_OPS[] = {
	init_time_fixed<0>, init_time_fixed<1>, init_time_fixed<2>,
	init_time_fixed<3>, init_time_fixed<4>, init_time_fixed<5>,
	init_time_fixed<6>, init_time_fixed<7>
};

static const update_func_t UPDATE_TIME_OPS[] = {
	update_time_fixed<0>, update_time_fixed<1>, update_time_fixed<2>,
	update_time_fixed<3>, update_time_fixed<4>, update_time_fixed<5>,
	update_time_fixed<6>, update_time_fixed<7>
};

/* --------------------------------------------------------------------------
 * 3. Engine Initialization
 * --------------------------------------------------------------------------
 */

struct trcache* engine_init(const struct validator_config& config)
{
	/* 1. Prepare candle configurations */
	int num_configs = config.candles.size();
	if (num_configs > MAX_CANDLE_SLOTS) {
		std::cerr << "[Engine] Error: Too many candle configs. Max: "
			  << MAX_CANDLE_SLOTS << std::endl;
		return nullptr;
	}

	/* Use vector with push_back to handle const members correctly */
	std::vector<trcache_candle_config> tr_configs;
	tr_configs.reserve(num_configs);

	for (int i = 0; i < num_configs; i++) {
		const auto& user_cfg = config.candles[i];

		/* Store threshold in global slot for callback access */
		g_slot_configs[i] = user_cfg;

		/* Prepare Ops Structures */
		trcache_candle_update_ops u_ops = {};
		trcache_batch_flush_ops f_ops = {};
		
		f_ops.flush = flush_noop;

		/* Map Logic Type to Slot Function */
		if (user_cfg.type == "TICK_MODULO") {
			u_ops.init = INIT_TICK_OPS[i];
			u_ops.update = UPDATE_TICK_OPS[i];
		} else if (user_cfg.type == "TIME_FIXED") {
			u_ops.init = INIT_TIME_OPS[i];
			u_ops.update = UPDATE_TIME_OPS[i];
		} else {
			std::cerr << "[Engine] Error: Unknown candle type: "
				  << user_cfg.type << std::endl;
			return nullptr;
		}

		/* * Initialize trcache_candle_config using aggregate init.
		 * This is required because update_ops and flush_ops are const.
		 */
		trcache_candle_config c_conf = {
			sizeof(val_candle),        /* user_candle_size */
			val_candle_fields,         /* field_definitions */
			num_val_candle_fields,     /* num_fields */
			u_ops,                     /* update_ops */
			f_ops                      /* flush_ops */
		};

		tr_configs.push_back(c_conf);
	}

	/* 2. Prepare Initialization Context */
	/*
	 * Convert MB to Bytes.
	 * 1ULL << 20 = 1024 * 1024 (1 MB)
	 */
	size_t mem_limit_bytes =
		config.memory_limit_mb * (1ULL << 20);

	trcache_init_ctx ctx;
	memset(&ctx, 0, sizeof(ctx));

	ctx.candle_configs = tr_configs.data();
	ctx.num_candle_configs = num_configs;
	ctx.batch_candle_count_pow2 = config.batch_size_pow2;
	ctx.cached_batch_count_pow2 = config.cached_batch_count_pow2;
	ctx.total_memory_limit = mem_limit_bytes;
	ctx.num_worker_threads = config.worker_threads;
	/* Add buffer to max_symbols for safety */
	ctx.max_symbols = (config.top_n > 0 ? config.top_n : 1000) + 50;

	/* 3. Initialize Engine */
	return trcache_init(&ctx);
}

void engine_destroy(struct trcache* cache)
{
	if (cache) {
		trcache_destroy(cache);
	}
}
