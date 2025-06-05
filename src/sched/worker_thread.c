/**
 * @file   worker_thread.c
 * @brief  Implementation of the worker thread main routine.
 */

#include "sched/worker_thread.h"
#include "utils/log.h"

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   state:     Target state structure.
 * @param   worker_id: Numeric identifier for the worker.
 *
 * @return  0 on success, -1 on failure.
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
	state->done = false;

	return 0;
}

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised worker_state pointer.
 */
void worker_state_destroy(struct worker_state *state)
{
	if (state == NULL) {
		return;
	}

	scq_destroy(state->sched_msg_queue);
	state->sched_msg_queue = NULL;
}

/**
 * @brief   Entry point for a worker thread.
 *
 * @param   cache:      Pointer to the global trcache instance.
 * @param   worker_id:  Numeric identifier for the worker.
 *
 * @return  0 on success, negative value on error.
 */
int worker_thread_main(struct trcache *cache, int worker_id)
{
	(void)cache;
	(void)worker_id;
	return 0;
}

