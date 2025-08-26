#ifndef ADMIN_THREAD_H
#define ADMIN_THREAD_H

#include "trcache.h"
#include "sched/sched_work_msg.h"

/**
 * admin_state - Runtime state for the admin thread.
 *
 * @sched_msg_queue: Queue of scheduler messages for admin commands.
 * @done:            Flag signalled during shutdown.
 */
struct admin_state {
	sched_work_msg_queue *sched_msg_queue;
	bool done;
};

/**
 * @brief   Period of the admin thread main loop in milliseconds.
 *
 * The admin thread wakes up at this interval to perform routine duties
 * such as updating pipeline statistics and balancing worker loads.
 * A millisecond granularity is used rather than a fixed one‑second sleep
 * so that callers can tune responsiveness as needed. The default
 * period corresponds to one second. Override this definition at
 * compile time to customise the admin thread frequency.
 */
#ifndef ADMIN_THREAD_PERIOD_MS
#define ADMIN_THREAD_PERIOD_MS 1000
#endif

/**
 * @brief   Externally visible timestamp maintained by the admin thread.
 *
 * On each wakeup, the admin thread records the current real‑time clock
 * (in milliseconds since the Unix epoch) into this global. Other threads may
 * read this variable to make time‑based decisions – for example, determining
 * whether a time‑interval candle has crossed its boundary. It is declared as
 * _Atomic to permit lock‑free loads.
 */
extern _Atomic uint64_t g_admin_current_ts_ms;

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
 * @param   cache:  Global cache instance.
 */
void update_all_pipeline_stats(struct trcache *cache);

/**
 * @brief   Estimate worker limits per pipeline stage.
 *
 * @param   cache:  Global cache instance.
 * @param   limits: Output array sized WORKER_STAT_STAGE_NUM.
 */
void compute_stage_limits(struct trcache *cache, int *limits);

/**
 * @brief   Derive worker start indices for each pipeline stage.
 *
 * Uses @limits to position stage worker ranges within the total worker pool.
 * When the worker count is too small, @limits is overwritten with fallback
 * values.
 *
 * @param   cache:  Global cache instance.
 * @param   limits: Per-stage worker limits.
 * @param   start:  Output array sized WORKER_STAT_STAGE_NUM.
 */
void compute_stage_starts(struct trcache *cache, int *limits, int *start);

#endif /* ADMIN_THREAD_H */
