/*
 * Example flush implementation using DuckDB.
 *
 * Requires the DuckDB client library (libduckdb-dev on Debian/Ubuntu).
 */

#include <duckdb.h>
#include "../../../trcache.h"

static void *duckdb_flush(struct trcache *cache,
			  struct trcache_candle_batch *batch,
			  void *ctx)
{
	duckdb_connection conn = ctx;
	duckdb_appender appender;
	if (duckdb_appender_create(conn, NULL, "candles", &appender))
		return NULL;

	for (int i = 0; i < batch->num_candles; i++) {
		duckdb_append_int64(appender, batch->start_timestamp_array[i]);
		duckdb_append_double(appender, batch->open_array[i]);
		duckdb_append_double(appender, batch->high_array[i]);
		duckdb_append_double(appender, batch->low_array[i]);
		duckdb_append_double(appender, batch->close_array[i]);
		duckdb_append_double(appender, batch->volume_array[i]);
		duckdb_appender_end_row(appender);
	}

	duckdb_appender_flush(appender);
	duckdb_appender_destroy(&appender);
	(void)cache;
	return NULL; /* synchronous completion */
}

struct trcache_flush_ops duckdb_flush_ops = {
	.flush = duckdb_flush,
};
