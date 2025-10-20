/**
 * @file   worker_thread.c
 * @brief  Implementation of the worker thread main routine.
 */

#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <stdatomic.h>

#include "sched/worker_thread.h"
#include "meta/symbol_table.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/hash_table.h"
#include "utils/log.h"
#include "utils/tsc_clock.h"

/**
 * @brief   Create a 64-bit identifier for a work item.
 *
 * Combines @symbol_id, @stage and @type (converted to base and index),
 * so it can be used as a hash table key.
 *
 * @param   symbol_id:  Numeric symbol identifier.
 * @param   stage:      Stage in which the work belongs.
 * @param   candle_idx: Candle type index.
 *
 * @return  Packed 64-bit key.
 */
static uint64_t pack_work_key(int symbol_id, worker_stat_stage_type stage,
	int candle_idx)
{
	return ((uint64_t)(uint32_t)symbol_id << 32) |
		((uint64_t)stage << 16) |
		(uint64_t)candle_idx;
}

/**
 * @brief   Track a newly assigned work item.
 *
 * @param   state:  Worker owning the item.
 * @param   key:    Packed work identifier.
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
	item->key.candle_idx = (int)(key & 0xFFFF);

	list_add_tail(&item->node, &state->work_list);
	ht_insert(state->work_map, (void *)key, sizeof(void *), item);
}

/**
 * @brief   Drop a work item from the worker's set.
 *
 * @param   state:  Worker owning the item.
 * @param   key:    Packed work identifier.
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
 * @brief   Process a SCHED_MSG_ADD_WORK message.
 *
 * @param   cache:  Global cache instance.
 * @param   state:  Worker receiving the work.
 * @param   cmd:    Work command payload.
 */
static void worker_handle_add_work(struct trcache *cache,
	struct worker_state *state, struct sched_work_cmd *cmd)
{
	struct symbol_entry *entry = symbol_table_lookup_entry(
		cache->symbol_table, cmd->symbol_id);
	int cur_val, expected = -1;
	uint64_t key;

	if (!entry) {
		return;
	}

	cur_val = atomic_load(&entry->in_progress[cmd->stage][cmd->candle_idx]);
	if (cur_val == -1) {
		if (atomic_compare_exchange_strong(
				&entry->in_progress[cmd->stage][cmd->candle_idx],
				&expected, state->worker_id)) {
			key = pack_work_key(cmd->symbol_id, cmd->stage, cmd->candle_idx);
			worker_insert_work(state, key);
		}
	}
}

/**
 * @brief   Process a SCHED_MSG_REMOVE_WORK message.
 *
 * @param   cache:  Global cache instance.
 * @param   state:  Worker owning the work.
 * @param   cmd:    Work command payload.
 */
static void worker_handle_remove_work(struct trcache *cache,
	struct worker_state *state, struct sched_work_cmd *cmd)
{
	struct symbol_entry *entry = symbol_table_lookup_entry(
		cache->symbol_table, cmd->symbol_id);
	uint64_t key;
	int cur;

	if (!entry) {
		return;
	}

	cur = atomic_load(&entry->in_progress[cmd->stage][cmd->candle_idx]);
	if (cur == state->worker_id) {
		atomic_store(&entry->in_progress[cmd->stage][cmd->candle_idx], -1);
	}
	key = pack_work_key(cmd->symbol_id, cmd->stage, cmd->candle_idx);
	worker_remove_work(state, key);
}

/**
 * @brief   Dispatch scheduler message handlers.
 *
 * @param   cache:  Global cache instance.
 * @param   state:  Worker receiving the message.
 * @param   msg:    Scheduler message to process.
 */
static void worker_process_msg(struct trcache *cache,
	struct worker_state *state, struct sched_work_msg *msg)
{
	struct sched_work_cmd *cmd = &msg->cmd;

	switch (msg->type) {
		case SCHED_MSG_ADD_WORK:
			worker_handle_add_work(cache, state, cmd);
			break;
		case SCHED_MSG_REMOVE_WORK:
			worker_handle_remove_work(cache, state, cmd);
			break;
		default:
			break;
	}
}

/**
 * @brief   Consume trade data and update row candles.
 *
 * @param   state:      Worker context.
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type index.
 */
static void worker_do_apply(struct worker_state *state,
	struct symbol_entry *entry, int candle_idx)
{
	struct trade_data_buffer *buf = entry->trd_buf;
	struct trade_data_buffer_cursor *cur;
	struct candle_chunk_list *list;
	struct trcache_trade_data *array = NULL;
	int count = 0;
	uint64_t start, work_count = 0;

	cur = trade_data_buffer_acquire_cursor(buf, candle_idx);
	if (cur == NULL) {
		return;
	}

	start = tsc_cycles();

	list = entry->candle_chunk_list_ptrs[candle_idx];

	while (trade_data_buffer_peek(buf, cur, &array, &count) && count > 0) {
		for (int i = 0; i < count; i++) {
			candle_chunk_list_apply_trade(list, &array[i]);
		}

		trade_data_buffer_consume(buf, cur, count);
		work_count += (uint64_t)count;
	}

	worker_stat_add_apply(&state->stat, candle_idx,
		tsc_cycles() - start, work_count);

	trade_data_buffer_release_cursor(cur);
}

/**
 * @brief   Convert row candles to a column batch.
 *
 * @param   state:      Worker context.
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type index.
 */
static void worker_do_convert(struct worker_state *state,
	struct symbol_entry *entry, int candle_idx)
{
	struct candle_chunk_list *list =
		entry->candle_chunk_list_ptrs[candle_idx];
	uint64_t start = tsc_cycles();
	int converted_count = candle_chunk_list_convert_to_column_batch(list);

	if (converted_count > 0) {
		worker_stat_add_convert(&state->stat, candle_idx,
			tsc_cycles() - start, (uint64_t)converted_count);
	}
}

/**
 * @brief   Flush converted batches.
 *
 * @param   state:      Worker context.
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type index.
 */
static void worker_do_flush(struct worker_state *state,
	struct symbol_entry *entry, int candle_idx)
{
	struct candle_chunk_list *list =
		entry->candle_chunk_list_ptrs[candle_idx];
	uint64_t start = tsc_cycles();
	int flushed_batch_count = candle_chunk_list_flush(list);

	if (flushed_batch_count) {
		worker_stat_add_flush(&state->stat, candle_idx,
			tsc_cycles() - start, (uint64_t)flushed_batch_count);
	}
}

/**
 * @brief   Run one work item according to its stage.
 *
 * @param   cache:  Global cache instance.
 * @param   state:  Worker executing the item.
 * @param   item:   Work item descriptor.
 */
static void worker_execute_item(struct trcache *cache,
	struct worker_state *state, struct worker_work_item *item)
{
	struct symbol_entry *entry = symbol_table_lookup_entry(
		cache->symbol_table, item->key.symbol_id);

	if (!entry) {
		return;
	}

	switch (item->key.stage) {
		case WORKER_STAT_STAGE_APPLY:
			worker_do_apply(state, entry, item->key.candle_idx);
			break;
		case WORKER_STAT_STAGE_CONVERT:
			worker_do_convert(state, entry, item->key.candle_idx);
			break;
		case WORKER_STAT_STAGE_FLUSH:
			worker_do_flush(state, entry, item->key.candle_idx);
			break;
		default:
			break;
	}
}

/**
 * @brief   Iterate over all work items once.
 *
 * @param   cache:  Global cache instance.
 * @param   state:  Worker executing the items.
 */
static void worker_run_all_work(struct trcache *cache,
	struct worker_state *state)
{
	struct list_head *pos = state->work_list.next;
	struct worker_work_item *item;
	
	while (pos != &state->work_list) {
		item = list_entry(pos, struct worker_work_item, node);
		worker_execute_item(cache, state, item);
		pos = pos->next;
	}
}

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   state:     Target state structure.
 * @param   tc: Owner of the admin state.
 * @param   worker_id: Numeric identifier for the worker.
 *
 * @return  0 on success, -1 on failure.
 */
int worker_state_init(struct trcache *tc, int worker_id)
{
	struct worker_state *state;

	if (!tc) {
		errmsg(stderr, "Invalid trcache pointer\n");
		return -1;
	}

	state = &tc->worker_state_arr[worker_id];
	state->worker_id = worker_id;

	worker_stat_reset(&state->stat);
	
	state->sched_msg_queue = scq_init(&tc->mem_acc);
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
 * Accepts a pointer to ::worker_thread_args.
 *
 * @param   arg: See ::worker_thread_args.
 *
 * @return  Always returns NULL.
 */
void *worker_thread_main(void *arg)
{
	struct worker_thread_args *args = (struct worker_thread_args *)arg;
	struct trcache *cache = args->cache;
	int worker_id = args->worker_id;
	struct worker_state *state = &cache->worker_state_arr[worker_id];
	struct sched_work_msg *msg = NULL;

	while (!state->done) {
		while (scq_dequeue(state->sched_msg_queue, (void **)&msg)) {
			worker_process_msg(cache, state, msg);
			sched_work_msg_recycle(cache->sched_msg_free_list, msg);
		}

		worker_run_all_work(cache, state);
	}

	return NULL;
}

