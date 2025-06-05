/**
 * @file   admin_thread.c
 * @brief  Implementation of the admin thread main routine.
 */

#include "sched/admin_thread.h"
#include "utils/log.h"

int admin_state_init(struct admin_state *state)
{
	if (!state) {
		return -1;
	}

	state->sched_msg_queue = scq_init();
	if (state->sched_msg_queue == NULL) {
		errmsg(stderr, "admin sched_msg_queue allocation failed\n");
		return -1;
	}
	state->done = false;

	return 0;
}

void admin_state_destroy(struct admin_state *state)
{
	if (state == NULL) {
		return;
	}

	scq_destroy(state->sched_msg_queue);
	state->sched_msg_queue = NULL;
}

/**
 * @brief   Entry point for the admin thread.
 *
 * @param   cache:   Pointer to the global trcache instance.
 *
 * @return  0 on success, negative value on error.
 */
int admin_thread_main(struct trcache *cache)
{
	(void)cache;
	return 0;
}
