#ifndef SCHED_PIPELINE_STATS_H
#define SCHED_PIPELINE_STATS_H

#include <stdint.h>

#include "trcache.h"

struct symbol_entry;

/*
 * sched_stage_snapshot - Snapshot of pipeline sequences for one candle type.
 *
 * @produced_seq:   Last sequence produced into the trade buffer.
 * @completed_seq:  Last candle finalized in row format.
 * @converted_seq:  Last candle converted to column batches.
 */
struct sched_stage_snapshot {
	uint64_t produced_seq;
	uint64_t completed_seq;
	uint64_t converted_seq;
};

/*
 * sched_stage_rate - Exponential moving-average throughput.
 *
 * @produced_rate:  EMA of trades produced per second.
 * @completed_rate: EMA of completed row candles per second.
 * @converted_rate: EMA of conversions to column batches per second.
 */
struct sched_stage_rate {
	uint64_t produced_rate;
	uint64_t completed_rate;
	uint64_t converted_rate;
};

/*
 * sched_pipeline_stats - Statistics for all pipeline stages of one symbol.
 *
 * @timestamp_ns: Monotonic timestamp when the stats were captured.
 * @stage_snaps: Array indexed by candle type holding per-stage sequences.
 * @stage_rates: Array indexed by candle type holding per-stage throughput.
 */
struct sched_pipeline_stats {
	uint64_t timestamp_ns;
	struct sched_stage_snapshot stage_snaps[NUM_CANDLE_BASES][MAX_CANDLE_TYPES_PER_BASE];
	struct sched_stage_rate stage_rates[NUM_CANDLE_BASES][MAX_CANDLE_TYPES_PER_BASE];
};

/**
 * @brief   Refresh pipeline snapshot and update throughput rates.
 *
 * @param   cache:  Global cache instance.
 * @param   entry:  Symbol entry whose counters are polled.
 *
 * The function fetches the latest stage counters from the symbol's pipeline
 * data structures, computes per-stage input rates, updates the exponential
 * moving averages in @entry->pipeline_stats.stage_rates and refreshes the
 * snapshot with the new values.
 */
void sched_pipeline_calc_rates(struct trcache *cache,
	struct symbol_entry *entry);

#endif /* SCHED_PIPELINE_STATS_H */
