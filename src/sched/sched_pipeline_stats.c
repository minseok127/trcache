/**
 * @file   sched_pipeline_stats.c
 * @brief  Implementation of pipeline statistics helpers.
 */
#define _GNU_SOURCE
#include <stdatomic.h>

#include "sched/sched_pipeline_stats.h"
#include "meta/symbol_table.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/tsc_clock.h"

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
void sched_pipeline_calc_rates(struct symbol_entry *entry)
{
	struct sched_pipeline_stats snapshot;
	struct sched_stage_rate rates[TRCACHE_NUM_CANDLE_TYPE];
	struct sched_pipeline_stats *prev = &entry->pipeline_stats;
	double dt_sec;

	snapshot.timestamp_ns = tsc_cycles_to_ns(tsc_cycles());

	for (int i = 0; i < TRCACHE_NUM_CANDLE_TYPE; i++) {
		struct candle_chunk_list *list = entry->candle_chunk_list_ptrs[i];
		struct sched_stage_snapshot stage = {0};

		stage.produced_seq = entry->trd_buf->produced_count;

		if (list != NULL) {
			uint64_t mutable_seq = atomic_load_explicit(&list->mutable_seq,
					memory_order_acquire);
			uint64_t last_seq_conv = atomic_load_explicit(&list->last_seq_converted,
					memory_order_acquire);

			stage.completed_seq = (mutable_seq == UINT64_MAX) ?
					UINT64_MAX : mutable_seq - 1;
			stage.converted_seq = last_seq_conv;
		}

		snapshot.stage_snaps[i] = stage;
	}

	dt_sec = (double)(snapshot.timestamp_ns - prev->timestamp_ns) / 1e9;

	for (int i = 0; i < TRCACHE_NUM_CANDLE_TYPE; i++) {
		const struct sched_stage_snapshot *newc = &snapshot.stage_snaps[i];
		const struct sched_stage_snapshot *oldc = &prev->stage_snaps[i];
		struct sched_stage_rate *r = &rates[i];

		if (prev->timestamp_ns == 0 || dt_sec <= 0.0) {
			r->produced_per_sec = 0.0;
			r->completed_per_sec = 0.0;
			r->converted_per_sec = 0.0;
		} else {
			r->produced_per_sec =
					(double)(newc->produced_seq - oldc->produced_seq) / dt_sec;
			r->completed_per_sec =
					(double)(newc->completed_seq - oldc->completed_seq) / dt_sec;
			r->converted_per_sec =
					(double)(newc->converted_seq - oldc->converted_seq) / dt_sec;
		}
	}

	prev->timestamp_ns = snapshot.timestamp_ns;
	for (int i = 0; i < TRCACHE_NUM_CANDLE_TYPE; i++) {
		prev->stage_snaps[i] = snapshot.stage_snaps[i];
		prev->stage_rates[i] = rates[i];
	}
}
