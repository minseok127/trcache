/**
 * @file   admin_thread.c
 * @brief  Implementation of the admin thread main routine.
 */

#include "sched/admin_thread.h"

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
