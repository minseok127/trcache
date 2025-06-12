/*
 * Example flush implementation writing Arrow IPC files.
 *
 * Requires the Apache Arrow C++ libraries (libarrow, libarrow_cuda, etc.).
 */

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include "../../../trcache.h"

static void *arrow_flush(struct trcache *cache,
			 struct trcache_candle_batch *batch,
			 void *ctx)
{
	const char *path = (const char *)ctx;
	arrow::Int64Builder ts;
	arrow::DoubleBuilder open, high, low, close, vol;

	for (int i = 0; i < batch->num_candles; i++) {
		ts.Append(batch->start_timestamp_array[i]);
		open.Append(batch->open_array[i]);
		high.Append(batch->high_array[i]);
		low.Append(batch->low_array[i]);
		close.Append(batch->close_array[i]);
		vol.Append(batch->volume_array[i]);
	}

	std::shared_ptr<arrow::Array> ts_a, open_a, high_a, low_a, close_a, vol_a;
	ts.Finish(&ts_a);
	open.Finish(&open_a);
	high.Finish(&high_a);
	low.Finish(&low_a);
	close.Finish(&close_a);
	vol.Finish(&vol_a);

	auto schema = arrow::schema({
		arrow::field("ts", arrow::int64()),
		arrow::field("open", arrow::float64()),
		arrow::field("high", arrow::float64()),
		arrow::field("low", arrow::float64()),
		arrow::field("close", arrow::float64()),
		arrow::field("volume", arrow::float64()),
	});

	auto table = arrow::Table::Make(schema,
					{ts_a, open_a, high_a, low_a, close_a, vol_a});

	std::shared_ptr<arrow::io::FileOutputStream> out;
	arrow::io::FileOutputStream::Open(path).Value(&out);
	arrow::ipc::feather::WriteTable(*table, out.get());
	(void)cache;
	return NULL; /* synchronous completion */
}

struct trcache_flush_ops arrow_flush_ops = {
	.flush = arrow_flush,
};
