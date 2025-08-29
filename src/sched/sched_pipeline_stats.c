/**
 * @file   sched_pipeline_stats.c
 * @brief  Implementation of pipeline statistics helpers.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <stdatomic.h>

#include "sched/sched_pipeline_stats.h"
#include "meta/symbol_table.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/tsc_clock.h"

#define RATE_EMA_SHIFT (7)    /* alpha = 1 / 2^7 */
#define RATE_EMA_MULT  (127)

/**
 * @brief   Update 64-bit exponential moving average.
 *
 * @param   ema:   Previous EMA value.
 * @param   val:   New sample value.
 *
 * @return  Updated EMA value.
 *
 * EMA(t) = val(t) * (1/128) + (1 - 1/128) * EMA(t-1)
 */
static uint64_t ema_update_u64(uint64_t ema, uint64_t val)
{
	return (val + RATE_EMA_MULT * ema) >> RATE_EMA_SHIFT;
}

/**
 * @brief   Capture pipeline counters for one candle type.
 *
 * @param   entry:   Symbol entry containing the pipeline.
 * @param   type:    Candle type identifier.
 * @param   stage:   Output snapshot structure.
 */
static void snapshot_stage(struct symbol_entry *entry, trcache_candle_type type,
	struct sched_stage_snapshot *stage)
{
	struct candle_chunk_list *list
		= entry->candle_chunk_list_ptrs[type.base][type.type_idx];
	uint64_t mutable_seq, last_seq_conv;

	assert(entry->trd_buf != NULL);
	assert(list != NULL);

	stage->produced_seq = entry->trd_buf->produced_count;

	mutable_seq = atomic_load_explicit(&list->mutable_seq, 
		memory_order_acquire);
	last_seq_conv = atomic_load_explicit(&list->last_seq_converted,
		memory_order_acquire);

	if (mutable_seq == UINT64_MAX) {
		stage->completed_seq = UINT64_MAX;
	} else if (mutable_seq == 0) {
		stage->completed_seq = 0;
	} else {
		stage->completed_seq = mutable_seq - 1;
	}

	stage->converted_seq = last_seq_conv;
}

/**
 * @brief   Update throughput EMA for a pipeline stage.
 *
 * @param   r:       Rate structure to update.
 * @param   newc:    New snapshot of counters.
 * @param   oldc:    Previous snapshot of counters.
 * @param   dt_ns:   Elapsed time between snapshots in nanoseconds.
 */
static void update_stage_rate(struct sched_stage_rate *r,
	const struct sched_stage_snapshot *newc,
	const struct sched_stage_snapshot *oldc, uint64_t dt_ns)
{
	__uint128_t tmp;
	uint64_t diff;
	
	if (dt_ns == 0) {
		r->produced_rate = 0;
		r->completed_rate = 0;
		r->converted_rate = 0;
		return;
	}

	diff = newc->produced_seq - oldc->produced_seq;
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->produced_rate = ema_update_u64(r->produced_rate, (uint64_t)tmp);

	diff = newc->completed_seq - oldc->completed_seq;
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->completed_rate = ema_update_u64(r->completed_rate, (uint64_t)tmp);

	diff = newc->converted_seq - oldc->converted_seq;
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->converted_rate = ema_update_u64(r->converted_rate, (uint64_t)tmp);
}

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
	struct symbol_entry *entry)
{
	struct sched_pipeline_stats snapshot = { 0, };
	struct sched_pipeline_stats *prev = &entry->pipeline_stats;
	uint64_t dt_ns;
	trcache_candle_type type;

	snapshot.timestamp_ns = tsc_cycles_to_ns(tsc_cycles());

	dt_ns = snapshot.timestamp_ns - prev->timestamp_ns;

	for (int i = 0; i < NUM_CANDLE_BASES; ++i) {
		for (int j = 0; j < cache->num_candle_types[i]; ++j) {
			type.base = i;
			type.type_idx = j;

			snapshot_stage(entry, type, &snapshot.stage_snaps[i][j]);

			update_stage_rate(&prev->stage_rates[i][j],
				&snapshot.stage_snaps[i][j], &prev->stage_snaps[i][j],
				(prev->timestamp_ns == 0) ? 0 : dt_ns);

			prev->stage_snaps[i][j] = snapshot.stage_snaps[i][j];
		}
	}

	prev->timestamp_ns = snapshot.timestamp_ns;
}
