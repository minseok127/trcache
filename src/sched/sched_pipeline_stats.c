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

#define RATE_EMA_SHIFT 7 /* alpha = 1 / 2^7 */

static uint64_t ema_update_u64(uint64_t ema, uint64_t val)
{
	   return ema + ((val - ema) >> RATE_EMA_SHIFT);
}

static void snapshot_stage(struct symbol_entry *entry, int idx,
			  struct sched_stage_snapshot *stage)
{
	   struct candle_chunk_list *list = entry->candle_chunk_list_ptrs[idx];

	   stage->produced_seq = entry->trd_buf->produced_count;

	   if (list != NULL) {
		   uint64_t mutable_seq = atomic_load_explicit(&list->mutable_seq,
					   memory_order_acquire);
		   uint64_t last_seq_conv = atomic_load_explicit(&list->last_seq_converted,
					   memory_order_acquire);

		   stage->completed_seq = (mutable_seq == UINT64_MAX) ?
					   UINT64_MAX : mutable_seq - 1;
		   stage->converted_seq = last_seq_conv;
	   } else {
		   stage->completed_seq = 0;
		   stage->converted_seq = 0;
	   }
}

static void update_stage_rate(struct sched_stage_rate *r,
				 const struct sched_stage_snapshot *newc,
				 const struct sched_stage_snapshot *oldc,
				 uint64_t dt_ns)
{
	   if (dt_ns == 0) {
		   r->produced_rate = 0;
		   r->completed_rate = 0;
		   r->converted_rate = 0;
		   return;
	   }

	   __uint128_t tmp;
	   uint64_t diff;

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
 * @brief	Refresh pipeline snapshot and update throughput rates.
 *
 * @param	entry: Symbol entry whose counters are polled.
 *
 * The function polls the symbol's pipeline counters, computes per-stage
 * throughput and updates the running exponential averages in
 * @entry->pipeline_stats.
 */
void sched_pipeline_calc_rates(struct symbol_entry *entry)
{
	   struct sched_pipeline_stats snapshot;
	   struct sched_pipeline_stats *prev = &entry->pipeline_stats;
	   uint64_t dt_ns;
	   int i;

	   snapshot.timestamp_ns = tsc_cycles_to_ns(tsc_cycles());

	   for (i = 0; i < TRCACHE_NUM_CANDLE_TYPE; i++)
		   snapshot_stage(entry, i, &snapshot.stage_snaps[i]);

	   dt_ns = snapshot.timestamp_ns - prev->timestamp_ns;

	   for (i = 0; i < TRCACHE_NUM_CANDLE_TYPE; i++) {
		   update_stage_rate(&prev->stage_rates[i],
				 &snapshot.stage_snaps[i],
				 &prev->stage_snaps[i],
				 (prev->timestamp_ns == 0) ? 0 : dt_ns);
		   prev->stage_snaps[i] = snapshot.stage_snaps[i];
	   }

	   prev->timestamp_ns = snapshot.timestamp_ns;
}
