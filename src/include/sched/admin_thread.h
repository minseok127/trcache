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
	double cost_flush;
};

/*
 * admin_state - Runtime state for the admin thread.
 *
 * @stage_snaps:              Pointer to an array of napshot structures.
 * @stage_rates:              Pointer to an array of rate structures.
 * @task_costs:               Admin-local array for task costs (EMA cycles).
 * @next_im_bitmaps:          Reusable buffer for calculating the next
 *                            In-Memory assignment.
 * @next_flush_bitmaps:       Reusable buffer for calculating the next
 *                            Flush assignment.
 * @current_im_bitmaps:       Stores a copy of the last published
 *                            In-Memory assignment.
 * @current_flush_bitmaps:    Stores a copy of the last published
 *                            Flush assignment.
 * @in_mem_words_per_worker:  Size (in uint64_t words) of a single
 *                            worker's In-Memory bitmap.
 * @flush_words_per_worker:   Size (in uint64_t words) of a single
 *                            worker's Flush bitmap.
 * @done:                     Flag signalled during shutdown.
 *
 * The arrays are indexed using: [type_idx * max_symbols + sym_idx]
 * to match the admin thread's loop (Type -> Symbol) and the
 * new ownership flag layout.
 */
struct admin_state {
	struct sched_stage_snapshot *stage_snaps;
	struct sched_stage_rate *stage_rates;
	struct admin_task_costs *task_costs;
	uint64_t *next_im_bitmaps;
	uint64_t *next_flush_bitmaps;
	uint64_t *current_im_bitmaps;
	uint64_t *current_flush_bitmaps;
	size_t in_mem_words_per_worker;
	size_t flush_words_per_worker;
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
