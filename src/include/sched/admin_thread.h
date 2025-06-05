#ifndef ADMIN_THREAD_H
#define ADMIN_THREAD_H

#include "trcache.h"
#include "sched/sched_msg.h"

/**
 * admin_state - Runtime state for the admin thread.
 *
 * @sched_msg_queue: Queue of scheduler messages for admin commands.
 * @done:            Flag signalled during shutdown.
 */
	struct admin_state {
	sched_msg_queue *sched_msg_queue;
	bool done;
};

/**
 * @brief   Initialise the admin thread state.
 *
 * @param   state   Target state structure.
 *
 * @return  0 on success, -1 on failure.
 */
int admin_state_init(struct admin_state *state);

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state   Previously initialised admin_state pointer.
 */
void admin_state_destroy(struct admin_state *state);

/**
 * @brief   Entry point for the admin thread.
 *
 * @param   cache:   Pointer to the global trcache instance.
 *
 * @return  0 on success, negative value on error.
 */
int admin_thread_main(struct trcache *cache);

#endif /* ADMIN_THREAD_H */
