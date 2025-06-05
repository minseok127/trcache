#ifndef ADMIN_THREAD_H
#define ADMIN_THREAD_H

#include "trcache.h"

/**
 * @brief   Entry point for the admin thread.
 *
 * @param   cache:   Pointer to the global trcache instance.
 *
 * @return  0 on success, negative value on error.
 */
int admin_thread_main(struct trcache *cache);

#endif /* ADMIN_THREAD_H */
