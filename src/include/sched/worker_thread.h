#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include "trcache.h"
#include "sched/worker_stat_board.h"
#include "sched/sched_msg.h"
#include "utils/list_head.h"
#include "utils/hash_table.h"

/*
 * worker_work_key - Hashable identifier for a work item.
 *
 * @symbol_id: ID of the symbol associated with the work.
 * @stage:     Worker pipeline stage.
 * @candle_idx:Candle type index for the stage.
 */
struct worker_work_key {
       int symbol_id;
       uint8_t stage;
       uint8_t candle_idx;
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
 * @worker_id:       Numeric ID assigned to the worker thread.
 * @stat:            Performance counters split per pipeline stage.
 * @sched_msg_queue: Queue for scheduler messages destined to this worker.
 * @done:            Flag signalled during shutdown.
 * @work_map:        Hash table tracking work owned by this worker.
 * @work_list:       List of work items for iteration order.
 */
struct worker_state {
	int worker_id;
	struct worker_stat_board stat;
	sched_msg_queue *sched_msg_queue;
	bool done;
	struct ht_hash_table *work_map;
	struct list_head work_list;
};

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   state:     Target state structure.
 * @param   worker_id: Numeric identifier for the worker.
 *
 * @return  0 on success, -1 on failure.
 */
int worker_state_init(struct worker_state *state, int worker_id);

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised worker_state pointer.
 */
void worker_state_destroy(struct worker_state *state);

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
