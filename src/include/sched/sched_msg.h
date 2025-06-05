#ifndef SCHED_MSG_H
#define SCHED_MSG_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/futex.h>

#include "concurrent/scalable_queue.h"
#include "sched/worker_stat_board.h"
#include "trcache.h"

/*
 * sched_ack - Acknowledgement object for synchronous messages.
 *
 * @done: 0->in‑flight, 1->success, -1->error code.
 * @err:  Numeric error code filled when @done == -1.
 * @ptr:  Generic return pointer for sucess path.
 *
 * The caller provides a pointer to an instance of this structure when it wants
 * to block until the callee has consumed–and processed–the message.
 *
 * Memory‑ordering contract:
 *   ‑ Callee performs 'atomic_store_explicit(&done, 1, memory_order_release)'.
 *   ‑ Caller spins / futex‑waits and then reads with
 *     'atomic_load_explicit(&done, memory_order_acquire)' to observe @res.
 */
struct sched_ack {
       _Atomic int done;
       union {
               int err;
               void *ptr;
       } res;
};

/** Message kinds recognised by the scheduler (expand as needed). */
typedef enum sched_msg_type {
	/*
	 * The implementation currently does not define any concrete
	 * message types.  However, an empty enum is illegal in C and
	 * prevents the project from building.  Provide a dummy value so
	 * that users can extend the list without hitting a compilation
	 * error.
	 */
	SCHED_MSG_NONE = 0,
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
 * sched_msg - Generic message wrapper.
 *
 * @type:    Dispath tag.
 * @payload: Type-specific data pointer.
 * @ack:     NULL -> async, non‑NULL -> sync‑call token.
 *
 * The message object will be managed by the global SCQ‑based free‑list. After
 * the consumer processes the message (and optionally signals an ack) it must
 * recycle the object via sched_msg_recycle() .
 */
struct sched_msg {
       enum sched_msg_type type;
       void *payload;
       struct sched_ack *ack;
};

typedef struct scalable_queue sched_msg_queue;
typedef struct scalable_queue sched_msg_free_list;

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
struct sched_msg *sched_msg_alloc(sched_msg_free_list *freelist);

/**
 * @brief   Return a message to its owning free‑list (exactly once).
 *
 * @param   freelist:  The same free‑list passed to sched_msg_alloc().
 * @param   msg:       Message pointer to recycle.
 */
void sched_msg_recycle(sched_msg_free_list *freelist, struct sched_msg *msg);

/**
 * @brief   Post an *asynchronous* message – fire‑and‑forget.
 *
 * @param   q:    Destination queue (e.g. admin <-> worker).
 * @param   msg:  Fully initialised message (ownership transfers to queue).
 *
 * @return  0 on success, -1 on error.
 */
int sched_post_async(sched_msg_queue *q, struct sched_msg *msg);

/**
 * @brief   Post a *synchronous* message and block until completion.
 *
 * @param   q:    Destination queue.
 * @param   msg:  Fully initialised message (@msg->ack != NULL).

 * @return  0 on success, -1 on error.
 */
int sched_post_sync(sched_msg_queue *q, struct sched_msg *msg);

/**
 * @brief   Busy‑wait + futex‑wait loop until @ack->done becomes non‑zero.
 *
 * Caller supplies @ack pointer it previously embedded in a sync message and
 * blocks indefinitely until the receiver calls sched_ack_ok() or
 * sched_ack_err().
 *
 * @return  0 if ack.done == 1 (success), or ack->res.err.
 */
static inline int sched_futex_wait(struct sched_ack *ack)
{
	while (atomic_load_explicit(&ack->done, memory_order_acquire) == 0) {
		syscall(SYS_futex, &ack->done, FUTEX_WAIT_PRIVATE, 0, NULL, NULL, 0);
	}
	return (ack->done == 1) ? 0 : ack->res.err;
}

/**
 * @brief   Wake **one** thread blocked on the futex word @word.
 */
static inline void sched_futex_wake(_Atomic int *word)
{
	syscall(SYS_futex, word, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
}

/**
 * @brief   Receiver marks *success* and wakes waiting sender.
 */
static inline void sched_ack_ok(struct sched_msg *m, void *ret_ptr)
{
	if (m && m->ack) {
		m->ack->res.ptr = ret_ptr;
		atomic_store_explicit(&m->ack->done, 1, memory_order_release);
		sched_futex_wake(&m->ack->done);
	}
}

/**
 * @brief   Receiver marks *error* (negative errno) and wakes sender.
 */
static inline void sched_ack_err(struct sched_msg *m, int err_code)
{
	if (m && m->ack) {
		m->ack->res.err = err_code;
		atomic_store_explicit(&m->ack->done, -1, memory_order_release);
		sched_futex_wake(&m->ack->done);
	}
}

#endif /* SCHED_MSG_H */
