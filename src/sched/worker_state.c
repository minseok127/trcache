#include "sched/worker_state.h"
#include "utils/log.h"

/**
 * @brief   Initialise per-worker state data.
 *
 * @param   state:     target worker_state to initialise.
 * @param   worker_id: numeric identifier for this worker.
 *
 * @return  0 on success, -1 on error.
 */
int worker_state_init(struct worker_state *state, int worker_id)
{
	if (!state) {
		return -1;
	}

	state->worker_id = worker_id;
	worker_stat_reset(&state->stat);
	state->sched_msg_queue = scq_init();
	if (state->sched_msg_queue == NULL) {
		errmsg(stderr, "sched_msg_queue allocation failed\n");
		return -1;
	}

	return 0;
}

/**
 * @brief   Tear down a worker_state instance.
 *
 * @param   state: pointer returned from worker_state_init().
 */
void worker_state_destroy(struct worker_state *state)
{
	if (state == NULL) {
		return;
	}

	scq_destroy(state->sched_msg_queue);
}
