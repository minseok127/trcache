/*
 * Example flush implementation using PostgreSQL.
 *
 * Requires the libpq client library (libpq-dev on Debian/Ubuntu).
 */

#include <libpq-fe.h>
#include <inttypes.h>
#include <stdio.h>
#include "../../../trcache.h"

static void *pg_flush(struct trcache *cache,
		      struct trcache_candle_batch *batch,
		      void *ctx)
{
	PGconn *conn = ctx;
	const char *paramValues[6];
	char ts[32], open[32], high[32], low[32], close[32], vol[32];

	for (int i = 0; i < batch->num_candles; i++) {
	snprintf(ts, sizeof(ts), "%" PRIu64,
		batch->start_timestamp_array[i]);
		snprintf(open, sizeof(open), "%f", batch->open_array[i]);
		snprintf(high, sizeof(high), "%f", batch->high_array[i]);
		snprintf(low, sizeof(low), "%f", batch->low_array[i]);
		snprintf(close, sizeof(close), "%f", batch->close_array[i]);
		snprintf(vol, sizeof(vol), "%f", batch->volume_array[i]);

		paramValues[0] = ts;
		paramValues[1] = open;
		paramValues[2] = high;
		paramValues[3] = low;
		paramValues[4] = close;
		paramValues[5] = vol;
	PGresult *res = PQexecParams(conn,
		"INSERT INTO candles(ts, open, high, low, close, volume)"
		"VALUES($1,$2,$3,$4,$5,$6)",
		6, NULL, paramValues, NULL, NULL, 0);
		PQclear(res);
	}

	(void)cache;
	return NULL; /* synchronous completion */
}

struct trcache_flush_ops pg_flush_ops = {
	.flush = pg_flush,
};
