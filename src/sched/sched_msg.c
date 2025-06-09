/**
 * @file   sched_msg.c
 * @brief  Minimal implementation of sched message primitives.
 *
 * This translation unit provides the *generic* building blocks that any
 * higher‑level scheduler component (admin thread, worker pool, user helper
 * thread) can rely on:
 *
 *   - message allocation / recycling from a free‑list
 *     queue (implemented with scalable_queue).
 *   - asynchronous message post (fire‑and‑forget).
 *   - synchronous message post (block the caller until receiver acks).
 *
 * The code purposefully avoids knowledge of concrete message semantics – it
 * only moves opaque @sched_msg objects through queues and uses futexes to
 * implement the sync‑ack rendezvous.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include "sched/sched_msg.h"
#include "utils/log.h"

/**
 * @brief   Obtain a message object from the specified free‑list.
 *
 * @param   freelist:  free‑list owned by a particular trcache instance.
 *
 * @return  Pointer to zero‑initialised sched_msg; NULL on OOM.
 *
 * If the free‑list is empty the helper falls back to @c malloc; callers may
 * optionally pre‑fill the list to avoid allocations in the hot path.
 */
struct sched_msg *sched_msg_alloc(sched_msg_free_list *freelist)
{
	struct sched_msg *msg = NULL;

	if (!freelist) {
		goto memalloc;
	}

	if (scq_dequeue(freelist, (void **)&msg)) {
		assert(msg != NULL);
		return msg;
	}

memalloc:

	/* Fallback – allocate fresh */
	msg = malloc(sizeof(struct sched_msg));
	if (msg == NULL) {
		errmsg(stderr, "Message allocation is failed\n");
		return NULL;
	}

	return msg;
}

/**
 * @brief   Return a message to its owning free‑list (exactly once).
 *
 * @param   freelist:  The same free‑list passed to sched_msg_alloc().
 * @param   msg:        Message pointer to recycle.
 */
void sched_msg_recycle(sched_msg_free_list *freelist, struct sched_msg *msg)
{
	if (freelist == NULL || msg == NULL) {
		errmsg(stderr, "Invalid arguments\n");
		return;
	}

	scq_enqueue(freelist, (void *)msg);
}

/**
 * @brief   Post an *asynchronous* message – fire‑and‑forget.
 *
 * @param   q:    Destination queue (e.g. admin <-> worker).
 * @param   msg:  Fully initialised message (ownership transfers to queue).
 *
 * @return  0 on success, -1 on error.
 */
int sched_post_msg(sched_msg_queue *q, struct sched_msg *msg)
{
	if (q == NULL || msg == NULL) {
		errmsg(stderr, "Invalid arguments\n");
		return -1;
	}

	msg->ack = NULL;

	scq_enqueue(q, (void *)msg);

	return 0;
}
