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
 * @brief   Initialise the admin thread state.
 *
 * @param   state:   Target state structure.
 *
 * @return  0 on success, -1 on failure.
 */
int admin_state_init(struct admin_state *state);

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

#endif /* ADMIN_THREAD_H */
