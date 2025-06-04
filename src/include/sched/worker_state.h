#ifndef WORKER_STATE_H
#define WORKER_STATE_H

#include "sched/worker_stat_board.h"

struct worker_state {
	int worker_id;
	struct worker_stat_board stat;
};

#endif /* WORKER_STATE_H */
