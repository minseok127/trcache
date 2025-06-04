#ifndef WORKER_STATE_H
#define WORKER_STATE_H

#include "sched/worker_stat_board.h"

/**
 * worker_state - Per-worker runtime data.
 *
 * @worker_id: Numeric ID assigned to the worker thread.
 * @stat:      Performance counters split per pipeline stage.
 */
struct worker_state {
	int worker_id;
	struct worker_stat_board stat;
};

#endif /* WORKER_STATE_H */
