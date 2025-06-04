#ifndef SCHED_PIPELINE_STATS_H
#define SCHED_PIPELINE_STATS_H

#include <stdint.h>

#include "trcache.h"

struct symbol_entry;

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
 * sched_stage_rate - Throughput metrics derived from stage counters.
 *
 * @produced_per_sec:   Rate of trade records produced.
 * @completed_per_sec:  Rate of completed row candles.
 * @converted_per_sec:  Rate of conversions to column batches.
 * @unflushed_per_sec:  Rate of batches waiting to be flushed.
 */
struct sched_stage_rate {
	double produced_per_sec;
	double completed_per_sec;
	double converted_per_sec;
	double unflushed_per_sec;
};

/*
 * sched_pipeline_stats - Statistics for all pipeline stages of one symbol.
 *
 * @timestamp_ns: Monotonic timestamp when the stats were captured.
 * @stage_counts: Array indexed by candle type holding per-stage counters.
 * @stage_rates:  Array indexed by candle type holding per-stage throughput.
 */
struct sched_pipeline_stats {
	uint64_t timestamp_ns;
	struct sched_stage_snapshot stage_counts[TRCACHE_NUM_CANDLE_TYPE];
	struct sched_stage_rate stage_rates[TRCACHE_NUM_CANDLE_TYPE];
	struct sched_pipeline_rate stage_rates;
};

/**
 * @brief   Refresh pipeline snapshot and update throughput rates.
 *
 * @param   entry: Symbol entry whose counters are polled.
 *
 * The function fetches the latest stage counters from the symbol's pipeline
 * data structures, computes input rates relative to @entry->pipeline_stats,
 * stores the results in @entry->pipeline_stats.stage_rates and updates the 
 * snapshot with the new values.
 */
void sched_pipeline_calc_rates(struct symbol_entry *entry);

#endif /* SCHED_PIPELINE_STATS_H */
