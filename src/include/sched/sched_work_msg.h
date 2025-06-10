#ifndef SCHED_WORK_MSG_H
#define SCHED_WORK_MSG_H
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include "concurrent/scalable_queue.h"
#include "sched/worker_stat_board.h"
#include "trcache.h"

/** Message kinds recognised by the scheduler. */
typedef enum sched_msg_type {
	SCHED_MSG_ADD_WORK,
	SCHED_MSG_REMOVE_WORK,
} sched_msg_type;

/*
 * sched_work_cmd - Work descriptor for scheduler messages.
 *
 * @symbol_id:   Identifier of the target symbol.
 * @stage:       Pipeline stage to execute.
 * @candle_type: Candle type parameter for apply/convert/flush.
 */
struct sched_work_cmd {
	int symbol_id;
	worker_stat_stage_type stage;
	trcache_candle_type candle_type;
};

/*
 * sched_work_msg - Generic message wrapper.
 *
 * @type:  Dispath tag.
 * @cmd:   Work command payload.
 *
 * The message object will be managed by the global SCQ‑based free‑list. After
 * the consumer processes the message (and optionally signals an ack) it must
 * recycle the object via sched_work_msg_recycle().
 */
struct sched_work_msg {
	enum sched_msg_type type;
	struct sched_work_cmd cmd;
};

typedef struct scalable_queue sched_work_msg_queue;
typedef struct scalable_queue sched_work_msg_free_list;

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
struct sched_work_msg *sched_work_msg_alloc(sched_work_msg_free_list *freelist);

/**
 * @brief   Return a message to its owning free‑list (exactly once).
 *
 * @param   freelist:  The same free‑list passed to sched_work_msg_alloc().
 * @param   msg:       Message pointer to recycle.
 */
void sched_work_msg_recycle(sched_work_msg_free_list *freelist,
       struct sched_work_msg *msg);

/**
 * @brief   Post an *asynchronous* message – fire‑and‑forget.
 *
 * @param   q:    Destination queue (e.g. admin <-> worker).
 * @param   msg:  Fully initialised message (ownership transfers to queue).
 *
 * @return  0 on success, -1 on error.
 */
int sched_post_work_msg(sched_work_msg_queue *q, struct sched_work_msg *msg);

#endif /* SCHED_WORK_MSG_H */
