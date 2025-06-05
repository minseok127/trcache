/**
 * @file   worker_thread.c
 * @brief  Implementation of the worker thread main routine.
 */

#include "sched/worker_thread.h"

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

