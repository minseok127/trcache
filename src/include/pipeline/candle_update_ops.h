#ifndef CANDLE_UPDATE_OPS_H
#define CANDLE_UPDATE_OPS_H

#include <stddef.h>
#include <stdint.h>

#include "trcache.h"

/*
 * candle_update_ops - Callbacks for updating candles.
 *
 * @init:      Init function that sets up the candle with the first trade data.
 * @update:    Update function that reflects trade data.
 * @is_closed: Checks if trade data can be applied to the candle.
 */
struct candle_update_ops {
	void (*init)(struct trcache_candle *c, struct trcache_trade_data *d);
	void (*update)(struct trcache_candle *c, struct trcache_trade_data *d);
	bool (*is_closed)(struct trcache_candle *c, struct trcache_trade_data *d);
};

/**
 * @brief   Return update operations for a given candle type.
 *
 * @param   type: Candle type identifier (must represent a single type).
 *
 * @return  Pointer to the corresponding candle_update_ops.
 */
const struct candle_update_ops *get_candle_update_ops(trcache_candle_type type);

#endif /* CANDLE_UPDATE_OPS_H */
