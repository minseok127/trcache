#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include "trcache.h"
#include "sched/worker_stat_board.h"
#include "sched/sched_work_msg.h"
#include "utils/list_head.h"
#include "utils/hash_table.h"

/*
 * worker_work_key - Hashable identifier for a work item.
 *
 * @symbol_id:  ID of the symbol associated with the work.
 * @candle_idx: Candle type index for the stage.
 * @stage:      Worker pipeline stage.
 */
struct worker_work_key {
	int symbol_id;
	int candle_idx;
	uint8_t stage;
};

/*
 * worker_work_item - Node stored in a worker's work list.
 *
 * @node: List linkage for iteration.
 * @key:  Work identifier used in the hash table.
 */
struct worker_work_item {
	struct list_head node;
	struct worker_work_key key;
};

/**
 * worker_state - Per-worker runtime data.
 *
 * @worker_id:           Numeric ID assigned to the worker thread.
 * @stat:                Performance counters split per pipeline stage.
 * @sched_msg_queue:     Queue for scheduler messages destined to this worker.
 * @done:                Flag signalled during shutdown.
 * @work_map:            Hash table tracking work owned by this worker.
 * @work_list:           List of work items for iteration order.
 */
struct worker_state {
	int worker_id;
	struct worker_stat_board stat;
	sched_work_msg_queue *sched_msg_queue;
	bool done;
	struct ht_hash_table *work_map;
	struct list_head work_list;
};

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   state:     Target state structure.
 * @param   tc: Owner of the admin state.
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
