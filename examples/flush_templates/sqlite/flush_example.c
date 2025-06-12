/*
 * Example flush implementation using SQLite.
 *
 * Requires the SQLite3 client library (libsqlite3-dev on Debian/Ubuntu).
 */

#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include "../../../trcache.h"

static void *sqlite_flush(struct trcache *cache,
			  struct trcache_candle_batch *batch,
			  void *ctx)
{
	sqlite3 *db = ctx;
	sqlite3_stmt *stmt = NULL;
	const char *sql =
		"INSERT INTO candles(ts, open, high, low, close, volume) "
		"VALUES(?, ?, ?, ?, ?, ?)";

	if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK)
		return NULL;

	for (int i = 0; i < batch->num_candles; i++) {
		sqlite3_bind_int64(stmt, 1, batch->start_timestamp_array[i]);
		sqlite3_bind_double(stmt, 2, batch->open_array[i]);
		sqlite3_bind_double(stmt, 3, batch->high_array[i]);
		sqlite3_bind_double(stmt, 4, batch->low_array[i]);
		sqlite3_bind_double(stmt, 5, batch->close_array[i]);
		sqlite3_bind_double(stmt, 6, batch->volume_array[i]);
		sqlite3_step(stmt);
		sqlite3_reset(stmt);
	}

	sqlite3_finalize(stmt);
	(void)cache;
	return NULL; /* synchronous completion */
}

struct trcache_flush_ops sqlite_flush_ops = {
	.flush = sqlite_flush,
};
