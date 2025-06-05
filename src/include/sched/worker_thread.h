#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include "trcache.h"

/**
 * @brief   Entry point for a worker thread.
 *
 * @param   cache:      Pointer to the global trcache instance.
 * @param   worker_id:  Numeric identifier for the worker.
 *
 * @return  0 on success, negative value on error.
 */
int worker_thread_main(struct trcache *cache, int worker_id);

#endif /* WORKER_THREAD_H */
