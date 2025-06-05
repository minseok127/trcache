#ifndef WORKER_STATE_H
#define WORKER_STATE_H

#include "sched/worker_stat_board.h"
#include "sched/sched_msg.h"

/**
 * worker_state - Per-worker runtime data.
 *
 * @worker_id: Numeric ID assigned to the worker thread.
 * @stat:      Performance counters split per pipeline stage.
 * @sched_msg_queue: Queue for scheduler messages destined to this worker.
 */
struct worker_state {
	int worker_id;
	struct worker_stat_board stat;
	sched_msg_queue *sched_msg_queue;
};

/**
 * @brief   Initialise per-worker state.
 *
 * @param   state:     pointer to worker_state to initialise.
 * @param   worker_id: numeric worker ID for this state.
 *
 * @return  0 on success, -1 on failure.
 */
int worker_state_init(struct worker_state *state, int worker_id);

/**
 * @brief   Destroy a worker_state instance.
 *
 * @param   state: pointer returned from worker_state_init().
 */
void worker_state_destroy(struct worker_state *state);


#endif /* WORKER_STATE_H */
