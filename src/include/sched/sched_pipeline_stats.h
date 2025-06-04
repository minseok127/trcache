#ifndef SCHED_PIPELINE_STATS_H
#define SCHED_PIPELINE_STATS_H

#include <stdint.h>

#include "trcache.h"

/*
 * sched_stage_snapshot - Snapshot of pipeline counters for one candle type.
 *
 * @produced_count:        Number of trade records produced into the buffer.
 * @num_completed:         Number of candles finalized in row format.
 * @num_converted:         Number of candles converted to columnar batches.
 * @unflushed_batch_count: Batches waiting to be flushed to storage.
 */
struct sched_stage_snapshot {
	uint64_t produced_count;
	uint64_t num_completed;
	uint64_t num_converted;
	int unflushed_batch_count;
};

/*
 * sched_pipeline_stats - Statistics for all pipeline stages of one symbol.
 *
 * @timestamp_ns: Monotonic timestamp when the stats were captured.
 * @stage_counts: Array indexed by candle type holding per-stage counters.
 */
struct sched_pipeline_stats {
	uint64_t timestamp_ns;
	struct sched_stage_snapshot stage_counts[TRCACHE_NUM_CANDLE_TYPE];
};

#endif /* SCHED_PIPELINE_STATS_H */
