#ifndef SCHED_PIPELINE_STATS_H
#define SCHED_PIPELINE_STATS_H

#include <stdint.h>

#include "trcache.h"

/*
 * sched_stage_snapshot - Snapshot of pipeline sequences for one candle type.
 *
 * @timestamp_ns:          Monotonic timestamp when the stats were captured.
 * @produced_seq:          Last sequence produced into the trade buffer.
 * @completed_seq:         Last candle finalized in row format.
 * @unflushed_batch_count: Number of batches converted but not yet flushed.
 */
struct sched_stage_snapshot {
	uint64_t timestamp_ns;
	uint64_t produced_seq;
	uint64_t completed_seq;
	int unflushed_batch_count;
};

/*
 * sched_stage_rate - Exponential moving-average throughput.
 *
 * @produced_rate:        EMA of trades/sec (input to APPLY).
 * @completed_rate:       EMA of completed candles/sec (input to CONVERT).
 * @flushable_batch_rate: EMA of batches becoming available for flush/sec
 *                        (input to FLUSH).
 */
struct sched_stage_rate {
	uint64_t produced_rate;
	uint64_t completed_rate;
	uint64_t flushable_batch_rate;
};

#endif /* SCHED_PIPELINE_STATS_H */
