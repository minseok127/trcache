/*
 * Example flush implementation using MySQL.
 *
 * Requires the MySQL client library (libmysqlclient-dev on Debian/Ubuntu).
 */

#include <mysql/mysql.h>
#include <stdio.h>
#include "../../../trcache.h"

static void *mysql_flush(struct trcache *cache,
			 struct trcache_candle_batch *batch,
			 void *ctx)
{
	MYSQL *conn = ctx;
	char sql[256];

	for (int i = 0; i < batch->num_candles; i++) {
		snprintf(sql, sizeof(sql),
			"INSERT INTO candles(ts, open, high, low, close, volume) "
			"VALUES(%llu, %f, %f, %f, %f, %f)",
			 (unsigned long long)batch->start_timestamp_array[i],
			 batch->open_array[i], batch->high_array[i],
			 batch->low_array[i], batch->close_array[i],
			 batch->volume_array[i]);
		mysql_query(conn, sql);
	}

	(void)cache;
	return NULL; /* synchronous completion */
}

struct trcache_flush_ops mysql_flush_ops = {
	.flush = mysql_flush,
};
