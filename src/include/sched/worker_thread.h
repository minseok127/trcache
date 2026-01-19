#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include <stdatomic.h>
#include <stdint.h>

#include "trcache.h"

/**
 * @brief Defines the operational group a worker belongs to.
 *
 * The admin thread sets this identifier to inform the worker which
 * bitmap (in-memory or flush) it should scan for work.
 */
enum worker_group_type {
	GROUP_IN_MEMORY,
	GROUP_FLUSH
};

/**
 * worker_state - Per-worker runtime data.
 *
 * @worker_id:         Numeric ID assigned to the worker thread.
 * @done:              Flag signalled during shutdown.
 * @group_id:          Identifier for the worker's current assigned group.
 * @in_memory_bitmap:  Pointer to the cacheline-aligned bitmap for
 *                     Apply/Convert tasks.
 * @flush_bitmap:      Pointer to the cacheline-aligned bitmap for
 *                     Flush tasks.
 */
struct worker_state {
	int worker_id;
	_Atomic bool done;
	_Atomic int group_id;
	uint64_t *in_memory_bitmap;
	uint64_t *flush_bitmap;
};

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   tc:        Owner of the admin state.
 * @param   worker_id: Numeric identifier for the worker.
 *
 * @return  0 on success, -1 on failure.
 */
int worker_state_init(struct trcache *tc, int worker_id);

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised worker_state pointer.
 */
void worker_state_destroy(struct worker_state *state);

/*
 * Arguments of the worker_thread_main().
 */
struct worker_thread_args {
	struct trcache *cache;
	int worker_id;
};

/**
 * @brief   Entry point for a worker thread.
 *
 * Expects a ::worker_thread_args pointer as its argument.
 *
 * @param   arg: Opaque pointer cast from ::worker_thread_args.
 *
 * @return  Always returns NULL.
 */
void *worker_thread_main(void *arg);

#endif /* WORKER_THREAD_H */
