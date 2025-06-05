/**
* @file   worker_thread.c
* @brief  Implementation of the worker thread main routine.
*/

#define _GNU_SOURCE
#include "sched/worker_thread.h"
#include "meta/symbol_table.h"
#include "utils/hash_table.h"
#include "utils/log.h"
#include <sched.h>
#include <stdlib.h>
#include <stdatomic.h>

/*
* pack_work_key - Create a 64-bit identifier for a work item.
*
* Combines @symbol_id, @stage and @type (converted to index) so it can
* be used as a hash table key.
*/
static uint64_t pack_work_key(int symbol_id, worker_stat_stage_type stage,
	trcache_candle_type type)
{
	int idx = worker_ct_to_idx(type);
	return ((uint64_t)(uint32_t)symbol_id << 32) |
		((uint64_t)stage << 16) |
		(uint64_t)idx;
}

/*
* worker_insert_work - Track a newly assigned work item.
*/
static void worker_insert_work(struct worker_state *state, uint64_t key)
{
	struct worker_work_item *item = malloc(sizeof(*item));

	if (item == NULL) {
		errmsg(stderr, "work item allocation failed\n");
		return;
	}

	item->key.symbol_id = (int)(key >> 32);
	item->key.stage = (uint8_t)((key >> 16) & 0xFF);
	item->key.candle_idx = (uint8_t)(key & 0xFF);

	list_add_tail(&item->node, &state->work_list);
	ht_insert(state->work_map, (void *)key, sizeof(void *), item);
}

/*
* worker_remove_work - Drop a work item from the worker's set.
*/
static void worker_remove_work(struct worker_state *state, uint64_t key)
{
	bool found = false;
	struct worker_work_item *item = ht_find(state->work_map, (void *)key,
		sizeof(void *), &found);

	if (!found) {
		return;
	}

	list_del(&item->node);
	ht_remove(state->work_map, (void *)key, sizeof(void *));
	free(item);
}

/**
* @brief   Initialise the worker thread state.
*
* @param   state:     Target state structure.
* @param   worker_id: Numeric identifier for the worker.
*
* @return  0 on success, -1 on failure.
*/
int worker_state_init(struct worker_state *state, int worker_id)
{
	if (!state) {
		return -1;
	}

	state->worker_id = worker_id;

	worker_stat_reset(&state->stat);
	
	state->sched_msg_queue = scq_init();
	if (state->sched_msg_queue == NULL) {
		errmsg(stderr, "sched_msg_queue allocation failed\n");
		return -1;
	}

	state->done = false;

	state->work_map = ht_create(128, 0, NULL, NULL, NULL, NULL);
	if (state->work_map == NULL) {
		errmsg(stderr, "work_map allocation failed\n");
		scq_destroy(state->sched_msg_queue);
		return -1;
	}
	
	INIT_LIST_HEAD(&state->work_list);

	return 0;
}

/**
* @brief   Destroy resources held by @state.
*
* @param   state:   Previously initialised worker_state pointer.
*/
void worker_state_destroy(struct worker_state *state)
{
	if (state == NULL) {
		return;
	}

	scq_destroy(state->sched_msg_queue);
	state->sched_msg_queue = NULL;

	if (state->work_map) {
		struct list_head *pos = state->work_list.next;
		struct list_head *n = NULL;
		struct worker_work_item *item = NULL;

		while (pos != &state->work_list) {
			n = pos->next;
			item = list_entry(pos, struct worker_work_item, node);
			list_del(pos);
			ht_remove(state->work_map, &item->key, sizeof(void *));
			free(item);
			pos = n;
		}

		ht_destroy(state->work_map);
		state->work_map = NULL;
	}
}

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
	struct worker_state *state = &cache->worker_state_arr[worker_id];
	struct sched_msg *msg = NULL;

	while (!atomic_load(state->done)) {
		while (scq_dequeue(state->sched_msg_queue, (void **)&msg)) {
			struct sched_work_cmd *cmd = msg->payload;
			struct symbol_entry *entry = NULL;
			uint64_t key;
			int idx;

			switch (msg->type) {
				case SCHED_MSG_ADD_WORK:
					entry 
						= symbol_table_lookup_entry(
							cache->symbol_table, cmd->symbol_id);
	if (entry) {
	idx = worker_ct_to_idx(cmd->candle_type);
	int expected = -1;
	if (atomic_compare_exchange_strong(
	&entry->in_progress[cmd->stage][idx],
	&expected, state->worker_id)) {
	key = pack_work_key(cmd->symbol_id,
	cmd->stage, cmd->candle_type);
	worker_insert_work(state, key);
	}
	}
	break;
	case SCHED_MSG_REMOVE_WORK:
	entry = symbol_table_lookup_entry(cache->symbol_table,
	cmd->symbol_id);
	if (entry) {
	idx = worker_ct_to_idx(cmd->candle_type);
	int cur = atomic_load(&entry->in_progress[cmd->stage][idx]);
	if (cur == state->worker_id) {
	atomic_store(&entry->in_progress[cmd->stage][idx], -1);
	}
	key = pack_work_key(cmd->symbol_id,
	cmd->stage, cmd->candle_type);
	worker_remove_work(state, key);
	}
	break;
	default:
	break;
	}

	sched_msg_recycle(cache->sched_msg_free_list, msg);
	} else {
	sched_yield();
	}
	}

	return 0;
}

