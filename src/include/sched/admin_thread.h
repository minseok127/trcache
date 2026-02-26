#ifndef ADMIN_THREAD_H
#define ADMIN_THREAD_H

#include "sched/sched_pipeline_stats.h"

#include "trcache.h"

/**
 * @brief Admin-local snapshot of task costs (EMA cycles per item).
 *
 * Stored in (candle_type, symbol_id) major order, matching
 * stage_snaps and stage_rates.
 */
struct admin_task_costs {
	double cost_apply;
	double cost_convert;
	double cost_batch_flush;
};

/*
 * admin_state - Runtime state for the admin thread.
 *
 * @stage_snaps:              Pointer to an array of snapshot structures.
 *                            Indexed [type_idx * max_symbols + sym_idx].
 * @stage_rates:              Pointer to an array of rate structures.
 *                            Indexed [type_idx * max_symbols + sym_idx].
 * @task_costs:               Admin-local array for task costs (EMA cycles).
 *                            Indexed [type_idx * max_symbols + sym_idx].
 * @trade_flush_costs:        Per-symbol EMA cycles per trade-chunk flush.
 *                            Indexed [sym_idx] only.
 * @trade_chunk_fill_rates:   Per-symbol EMA of trade chunks filled per
 *                            second. Indexed [sym_idx] only; independent
 *                            of candle type.
 * @trade_fill_timestamps:    Per-symbol timestamp (ns) of the last
 *                            trade_chunk_fill_rates update. Indexed
 *                            [sym_idx] only.
 * @next_im_bitmaps:               Reusable buffer for calculating the next
 *                                 In-Memory assignment.
 * @next_batch_flush_bitmaps:      Reusable buffer for calculating the next
 *                                 Batch Flush assignment.
 * @next_trade_flush_bitmaps:      Reusable buffer for calculating the next
 *                                 trade-chunk Flush assignment.
 * @current_im_bitmaps:            Stores a copy of the last published
 *                                 In-Memory assignment.
 * @current_batch_flush_bitmaps:         Stores a copy of the last published
 *                                 Flush assignment.
 * @current_trade_flush_bitmaps:   Stores a copy of the last published
 *                                 trade-chunk Flush assignment.
 * @in_mem_words_per_worker:       Size (in uint64_t words) of a single
 *                                 worker's In-Memory bitmap.
 * @batch_flush_words_per_worker:        Size (in uint64_t words) of a single
 *                                 worker's Flush bitmap.
 * @trade_flush_words_per_worker:  Size (in uint64_t words) of a single
 *                                 worker's trade-chunk Flush bitmap.
 * @done:                          Flag signalled during shutdown.
 */
struct admin_state {
	struct sched_stage_snapshot *stage_snaps;
	struct sched_stage_rate *stage_rates;
	struct admin_task_costs *task_costs;
	double *trade_flush_costs;
	uint64_t *trade_chunk_fill_rates;
	uint64_t *trade_fill_timestamps;
	uint64_t *next_im_bitmaps;
	uint64_t *next_batch_flush_bitmaps;
	uint64_t *next_trade_flush_bitmaps;
	uint64_t *current_im_bitmaps;
	uint64_t *current_batch_flush_bitmaps;
	uint64_t *current_trade_flush_bitmaps;
	size_t in_mem_words_per_worker;
	size_t batch_flush_words_per_worker;
	size_t trade_flush_words_per_worker;
	_Atomic(bool) done;
};

/**
 * @brief   Initialise the admin thread state.
 *
 * @param   tc: Owner of the admin state.
 *
 * @return  0 on success, -1 on failure.
 */
int admin_state_init(struct trcache *tc);

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised admin_state pointer.
 */
void admin_state_destroy(struct admin_state *state);

/**
 * @brief   Entry point for the admin thread.
 *
 * Expects a ::trcache pointer as its argument.
 *
 * @param   arg: Pointer to ::trcache.
 *
 * @return  Always returns NULL.
 */
void *admin_thread_main(void *arg);

/**
 * @brief   Refresh pipeline statistics for all symbols.
 *
 * This function polls all symbol entries and updates the admin-thread-local
 * statistics arrays in 'admin_state'.
 *
 * @param   cache:  Global cache instance.
 */
void update_all_pipeline_stats(struct trcache *cache);

#endif /* ADMIN_THREAD_H */
