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

int worker_state_init(struct worker_state *state, int worker_id);
void worker_state_destroy(struct worker_state *state);

#endif /* WORKER_STATE_H */
