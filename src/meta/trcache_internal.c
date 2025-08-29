/**
 * @file core/trcache_internal.c
 * @brief APIs for trcache, and thread-local cache management for trcache.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>

#include "meta/trcache_internal.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/hash_table_callbacks.h"
#include "utils/log.h"
#include "utils/tsc_clock.h"
#include "sched/sched_work_msg.h"
#include "sched/admin_thread.h"

#include "trcache.h"

/**
 * @brief   Retrieve or create thread-local data for this trcache instance.
 *
 * If TLS already exists, returns it. Otherwise allocates a new
 * trcache_tls_data, assigns a unique thread_id under mutex, and installs
 * it via pthread_setspecific.
 *
 * @param   tc: Pointer to trcache instance.
 *
 * @return  Pointer to initialized trcache_tls_data, or NULL on error.
 */
static struct trcache_tls_data *get_tls_data_or_create(struct trcache *tc)
{
	struct trcache_tls_data *tls_data_ptr
		= pthread_getspecific(tc->pthread_trcache_key);

	/* Already initialized, return it */
	if (tls_data_ptr != NULL) {
		return tls_data_ptr;
	}

	tls_data_ptr = malloc(sizeof(struct trcache_tls_data));
	if (tls_data_ptr == NULL) {
		errmsg(stderr, "#trcache_tls_data allocation failed\n");
		return NULL;
	}

	tls_data_ptr->local_symbol_id_map = NULL;
	tls_data_ptr->local_trd_databuf_map = NULL;
	tls_data_ptr->trcache_ptr = tc;
	tls_data_ptr->thread_id = -1;

	/* Acquire a free thread ID */
	pthread_mutex_lock(&tc->tls_id_mutex);
	for (int i = 0; i < MAX_NUM_THREADS; i++) {
		if (atomic_load(&tc->tls_id_assigned_flag[i]) == 0) {
			atomic_store(&tc->tls_id_assigned_flag[i], 1);
			atomic_store(&tc->tls_data_ptr_arr[i], tls_data_ptr);
			tls_data_ptr->thread_id = i;
			break;
		}
	}
	pthread_mutex_unlock(&tc->tls_id_mutex);

	if (tls_data_ptr->thread_id == -1) {
		errmsg(stderr, "Invalid thread ID\n");
		free(tls_data_ptr);
		return NULL;
	}

	/* Set argument of trcache_per_thread_destructor() */
	pthread_setspecific(tc->pthread_trcache_key, (void *)tls_data_ptr);

	INIT_LIST_HEAD(&tls_data_ptr->local_free_list);

	return tls_data_ptr;
}

/**
 * @brief   Clean up per-thread data (called by destructor or trcache_destroy).
 *
 * @param   tls: Pointer to trcache_tls_data to free.
 */
static void destroy_tls_data(struct trcache_tls_data *tls_data)
{
	struct trade_data_chunk *chunk = NULL;
	struct list_head *c = NULL, *n = NULL;

	if (tls_data->local_symbol_id_map != NULL) {
		ht_destroy(tls_data->local_symbol_id_map);
	}

	if (tls_data->local_trd_databuf_map != NULL) {
		ht_destroy(tls_data->local_trd_databuf_map);
	}

	if (!list_empty(&tls_data->local_free_list)) {
		c = list_get_first(&tls_data->local_free_list);
		while (c != &tls_data->local_free_list) {
			n = c->next;
			chunk = __get_trd_chunk_ptr(c);
			free(chunk);
			c = n;
		}
	}

	free(tls_data);
}

/**
 * @brief   TLS destructor invoked on thread exit.
 *
 * Returns thread_id and TLS pointer to pool, then frees data.
 *
 * @param   value: TLS pointer from pthread_getspecific.
 */
static void trcache_per_thread_destructor(void *value)
{
	struct trcache_tls_data *tls_data_ptr = (struct trcache_tls_data *)value;
	struct trcache *tc = tls_data_ptr->trcache_ptr;

	/* Give back the thread id into the trcache */
	pthread_mutex_lock(&tc->tls_id_mutex);
	atomic_store(&tc->tls_id_assigned_flag[tls_data_ptr->thread_id], 0);
	atomic_store(&tc->tls_data_ptr_arr[tls_data_ptr->thread_id], NULL);
	pthread_mutex_unlock(&tc->tls_id_mutex);

	destroy_tls_data(tls_data_ptr);
}

/**
 * @brief   Initialize trcache, set up TLS key and symbol table.
 *
 * @param   ctx: Pointer to a fully-initialised #trcache_init_ctx.
 *
 * @return  Pointer to new trcache, or NULL on failure.
 */
struct trcache *trcache_init(const struct trcache_init_ctx *ctx)
{
	struct trcache *tc = calloc(1, sizeof(struct trcache));
	int ret;

	if (tc == NULL) {
		errmsg(stderr, "#trcache allocation failed\n");
		return NULL;
	}

	/* Create TLS key with destructor */
	ret = pthread_key_create(&tc->pthread_trcache_key,
		trcache_per_thread_destructor);
	if (ret != 0) {
		errmsg(stderr, "Failure on pthread_key_create()\n");
		free(tc);
		return NULL;
	}

	/* Initialize shared symbol table */
	tc->symbol_table = symbol_table_init(1024);
	if (tc->symbol_table == NULL) {
		errmsg(stderr, "Failure on symbol_table_init()\n");
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	for (int i = 0; i < NUM_CANDLE_BASES; i++) {
		if (ctx->num_candle_types[i] > MAX_CANDLE_TYPES_PER_BASE) {
			errmsg(stderr, "Too many candle types for base %d. MAX is %d\n",
				i, MAX_CANDLE_TYPES_PER_BASE);
			symbol_table_destroy(tc->symbol_table, tc);
			pthread_key_delete(&tc->pthread_trcache_key);
			free(tc);
			return NULL;
		}

		tc->num_candle_types[i] = ctx->num_candle_types[i];
		if (ctx->candle_types[i] && tc->num_candle_types[i] > 0) {
			memcpy(tc->candle_configs[i], ctx->candle_types[i],
				sizeof(trcache_candle_config) * tc->num_candle_types[i]);
		}
	}

	tc->num_workers = ctx->num_worker_threads;
	tc->batch_candle_count_pow2 = ctx->batch_candle_count_pow2;
	tc->batch_candle_count = (1 << ctx->batch_candle_count_pow2);
	tc->flush_threshold_pow2 = ctx->cached_batch_count_pow2;
	tc->flush_threshold = (1 << ctx->cached_batch_count_pow2);
	tc->mem_acc.aux_limit = ctx->aux_memory_limit;

	pthread_mutex_init(&tc->tls_id_mutex, NULL);

	tc->sched_msg_free_list = scq_init(&tc->mem_acc);
	if (tc->sched_msg_free_list == NULL) {
		errmsg(stderr, "sched_msg_free_list allocation failed\n");
		pthread_mutex_destroy(&tc->tls_id_mutex);
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	if (admin_state_init(tc) != 0) {
		errmsg(stderr, "admin_state_init failed\n");
		scq_destroy(tc->sched_msg_free_list);
		pthread_mutex_destroy(&tc->tls_id_mutex);
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	tc->worker_state_arr = calloc(tc->num_workers, sizeof(struct worker_state));
	if (tc->worker_state_arr == NULL) {
		errmsg(stderr, "worker_state_arr allocation failed\n");
		pthread_mutex_destroy(&tc->tls_id_mutex);
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	for (int i = 0; i < tc->num_workers; i++) {
		if (worker_state_init(tc, i) != 0) {
			errmsg(stderr, "worker_state_init failed\n");
			for (int j = 0; j < i; j++) {
				worker_state_destroy(&tc->worker_state_arr[j]);
			}
			free(tc->worker_state_arr);
			scq_destroy(tc->sched_msg_free_list);
			pthread_mutex_destroy(&tc->tls_id_mutex);
			symbol_table_destroy(tc->symbol_table);
			pthread_key_delete(tc->pthread_trcache_key);
			free(tc);
			return NULL;
		}
	}

	tc->worker_threads = calloc(tc->num_workers, sizeof(pthread_t));
	tc->worker_args = calloc(tc->num_workers,
		sizeof(struct worker_thread_args));
	if (tc->worker_threads == NULL || tc->worker_args == NULL) {
		errmsg(stderr, "worker thread resources allocation failed\n");
		free(tc->worker_threads);
		free(tc->worker_args);
		for (int i = 0; i < tc->num_workers; i++) {
			worker_state_destroy(&tc->worker_state_arr[i]);
		}
		free(tc->worker_state_arr);
		admin_state_destroy(&tc->admin_state);
		scq_destroy(tc->sched_msg_free_list);
		pthread_mutex_destroy(&tc->tls_id_mutex);
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	for (int i = 0; i < tc->num_workers; i++) {
		tc->worker_args[i].cache = tc;
		tc->worker_args[i].worker_id = i;

		ret = pthread_create(&tc->worker_threads[i], NULL,
				worker_thread_main, &tc->worker_args[i]);
		if (ret != 0) {
			errmsg(stderr, "Failure on pthread_create() for worker\n");
			tc->admin_state.done = true;
			pthread_join(tc->admin_thread, NULL);
			for (int j = 0; j < i; j++) {
				tc->worker_state_arr[j].done = true;
				pthread_join(tc->worker_threads[j], NULL);
			}
			free(tc->worker_threads);
			free(tc->worker_args);
			for (int j = 0; j < tc->num_workers; j++) {
				worker_state_destroy(&tc->worker_state_arr[j]);
			}
			free(tc->worker_state_arr);
			admin_state_destroy(&tc->admin_state);
			scq_destroy(tc->sched_msg_free_list);
			pthread_mutex_destroy(&tc->tls_id_mutex);
			symbol_table_destroy(tc->symbol_table);
			pthread_key_delete(tc->pthread_trcache_key);
			free(tc);
			return NULL;
		}
	}

	ret = pthread_create(&tc->admin_thread, NULL, admin_thread_main, tc);
	if (ret != 0) {
		errmsg(stderr, "Failure on pthread_create() for admin\n");
		free(tc->worker_threads);
		free(tc->worker_args);
		for (int i = 0; i < tc->num_workers; i++) {
			worker_state_destroy(&tc->worker_state_arr[i]);
		}
		free(tc->worker_state_arr);
		admin_state_destroy(&tc->admin_state);
		scq_destroy(tc->sched_msg_free_list);
		pthread_mutex_destroy(&tc->tls_id_mutex);
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	return tc;
}

/**
 * @brief   Destroy trcache, freeing all resources including TLS data.
 *
 * @param   tc: Pointer to trcache to destroy.
 */
void trcache_destroy(struct trcache *tc)
{
	if (tc == NULL) {
		return;
	}

	/* stop all threads */
	tc->admin_state.done = true;
	for (int i = 0; i < tc->num_workers; i++) {
		tc->worker_state_arr[i].done = true;
	}

	pthread_join(tc->admin_thread, NULL);
	for (int i = 0; i < tc->num_workers; i++) {
		pthread_join(tc->worker_threads[i], NULL);
	}

	/* Return back trcache id */
	pthread_key_delete(tc->pthread_trcache_key);

	for (int i = 0; i < MAX_NUM_THREADS; i++) {
		if (tc->tls_data_ptr_arr[i] != NULL) {
			destroy_tls_data(tc->tls_data_ptr_arr[i]);
		}
	}

	pthread_mutex_destroy(&tc->tls_id_mutex);
	
	symbol_table_destroy(tc->symbol_table);
	admin_state_destroy(&tc->admin_state);

	for (int i = 0; i < tc->num_workers; i++) {
		worker_state_destroy(&tc->worker_state_arr[i]);
	}
	
	scq_destroy(tc->sched_msg_free_list);

	free(tc->worker_state_arr);
	free(tc->worker_threads);
	free(tc->worker_args);

	free(tc);
}


/**
 * @brief   Resolve symbol ID from string using TLS cache and shared table.
 *
 * @param   tc:          Pointer to trcache instance.
 * @param   tls:         Thread local storage pointer.
 * @param   symbol_str:  NULL-terminated symbol string.
 *
 * @return  Symbol ID on success, -1 on failure.
 */
static int resolve_symbol_id(struct trcache *tc, struct trcache_tls_data *tls,
	const char *symbol_str)
{
	bool found = false;
	int symbol_id = -1;

	if (tls->local_symbol_id_map == NULL) {
		tls->local_symbol_id_map = ht_create(1024, 0xDEADBEEFULL,
			murmur_hash, compare_symbol_str, duplicate_symbol_str,
			free_symbol_str);
		if (tls->local_symbol_id_map == NULL) {
			errmsg(stderr, "Failure on ht_create()\n");
			return -1;
		}
	}

	symbol_id = (int)(uintptr_t)ht_find(tls->local_symbol_id_map, symbol_str,
		strlen(symbol_str) + 1, &found);

	if (!found) {
		symbol_id = symbol_table_lookup_symbol_id(
			tc->symbol_table, symbol_str);
		   
		if (symbol_id == -1) {
			return -1;
		}
		   
		if (ht_insert(tls->local_symbol_id_map, symbol_str,
				strlen(symbol_str) + 1,
				(void *)(uintptr_t)symbol_id) < 0) {
			errmsg(stderr, "Failure on ht_insert()\n");
			return -1;
		}
	}

	return symbol_id;
}
/**
 * @brief   Register symbol string via TLS cache or shared table.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated string.
 *
 * @return  Symbol ID >=0, or -1 on error.
 */
int trcache_register_symbol(struct trcache *tc, const char *symbol_str)
{
	struct trcache_tls_data *tls_data_ptr = get_tls_data_or_create(tc);
	int symbol_id;

	symbol_id = resolve_symbol_id(tc, tls_data_ptr, symbol_str);
	if (symbol_id != -1) {
		return symbol_id;
	}

	symbol_id = symbol_table_register(tc, tc->symbol_table, symbol_str);
	if (symbol_id == -1) {
		return -1;
	}

	if (ht_insert(tls_data_ptr->local_symbol_id_map, symbol_str,
			strlen(symbol_str) + 1, (void *)(uintptr_t)symbol_id) < 0) {
		errmsg(stderr, "Failure on ht_insert()\n");
		return -1;
	}

	return symbol_id;
}


/**
 * @brief   Lookup symbol string by its symbol id.
 *
 * @param   tc:         Handle from trcache_init().
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 *
 * @return  NULL_terminated symbol string.
 */
const char *trcache_lookup_symbol_str(struct trcache *tc, int symbol_id)
{
	struct symbol_entry *entry
		= symbol_table_lookup_entry(tc->symbol_table, symbol_id);

	if (entry == NULL) {
		return NULL;
	}

	return entry->symbol_str;
}

/**
 * @brief   Lookup symbol ID by its symbol string using TLS cache.
 *
 * @param   tc:         Handle from trcache_init().
 * @param   symbol_str: NULL-terminated symbol string.
 *
 * @return  Symbol ID on success, -1 on failure.
 */
int trcache_lookup_symbol_id(struct trcache *tc, const char *symbol_str)
{
	struct trcache_tls_data *tls = get_tls_data_or_create(tc);

	if (tls == NULL) {
		return -1;
	}

	return resolve_symbol_id(tc, tls, symbol_str);
}

/**
 * @brief   Obtain candle chunk list for given symbol and type.
 */
static struct candle_chunk_list *get_chunk_list(struct trcache *tc,
	int symbol_id, trcache_candle_type type)
{
	struct symbol_entry *entry;
	int bit;

	entry = symbol_table_lookup_entry(tc->symbol_table, symbol_id);
	if (entry == NULL) {
		errmsg(stderr, "Invalid symbol id\n");
		return NULL;
	}

	bit = __builtin_ctz(type);
	if (bit < 0 || bit >= TRCACHE_NUM_CANDLE_TYPE) {
		errmsg(stderr, "Invalid candle type\n");
		return NULL;
	}

	return entry->candle_chunk_list_ptrs[bit];
}

/*
 * @brief   Determine whether a time‑based candle of a given index is near
 *          completion.
 *
 * A time‑based candle is considered "near completion" when the remaining
 * time in its interval is less than or equal to the admin thread period.
 * In such a state, applying buffered trades early helps ensure that
 * readers observe a fully updated candle immediately after the interval
 * rolls over.
 *
 * @param   idx:      Zero‑based index into the time‑based candle array.
 * @param   admin_ts: Current admin timestamp in milliseconds.
 *
 * @return  true if the candle is in its final admin period, otherwise false.
 */
static inline bool time_candle_should_apply(int idx, uint64_t admin_ts)
{
	uint64_t interval = candle_time_intervals_ms[idx];
	uint64_t remainder = admin_ts % interval;
	uint64_t time_left = interval - remainder;
	return time_left <= ADMIN_THREAD_PERIOD_MS;
}

/*
 * @brief   Determine whether a tick‑based candle of a given index is closed by
 *          this trade.
 *
 * For tick‑based candles, each candle contains a fixed number of trades
 * (the interval). When a trade arrives whose trade_id modulo the
 * interval equals interval − 1, that trade is the last trade of the
 * candle. Applying buffered trades at this point ensures the candle
 * reflects all trades before it is read.
 *
 * @param   idx:     Zero‑based index into the tick‑based candle array.
 * @param   trade_id: Identifier of the incoming trade.
 *
 * @return  true if this trade is the last trade in the candle, otherwise false.
 */
static inline bool tick_candle_should_apply(int idx, uint64_t trade_id)
{
	int interval_tick = candle_tick_intervals[idx];
	return (trade_id % (uint64_t)interval_tick)
		== (uint64_t)(interval_tick - 1);
}

/*
 * @brief   Opportunistically apply buffered trades for a given symbol.
 *
 * This helper encapsulates the logic for determining when to apply
 * buffered trades based on candle completion proximity. It examines
 * each candle type enabled in the cache, decides whether the incoming
 * trade implies closure of that candle (tick‑based) or whether the
 * current time is within the final admin period before a time‑based
 * candle rolls over, and applies the trades when appropriate.
 *
 * @param   tc:        Cache instance containing candle configuration.
 * @param   buf:       Trade data buffer associated with @symbol_id.
 * @param   symbol_id: Symbol identifier to look up candle lists.
 * @param   data:      Incoming trade.  Its trade_id is used to detect
 *                     tick‑based candle completion.
 */
static void maybe_apply_trades(struct trcache *tc,
	struct trade_data_buffer *buf, int symbol_id,
	struct trcache_trade_data *data)
{
	uint64_t admin_ts = atomic_load_explicit(&g_admin_current_ts_ms,
		memory_order_acquire);

	/* Iterate over all candle types set in the bitmask. */
	for (uint32_t m = tc->candle_type_flags; m != 0; m &= (m - 1)) {
		int idx = __builtin_ctz(m);
		trcache_candle_type type = (trcache_candle_type)(1U << idx);
		bool should_apply = false;

		if (idx < NUM_TIME_BASED_CANDLE_TYPES) {
			should_apply = time_candle_should_apply(idx, admin_ts);
		} else {
			int tick_idx = idx - NUM_TIME_BASED_CANDLE_TYPES;
			should_apply = tick_candle_should_apply(tick_idx, data->trade_id);
		}

		if (!should_apply) {
			continue;
		}

		/* 
		 * Acquire a cursor for this candle type.
		 * Skip if another worker thread holds it.
		 */
		struct trade_data_buffer_cursor *cur
			= trade_data_buffer_acquire_cursor(buf, type);
		if (cur == NULL) {
			continue;
		}

		struct candle_chunk_list *list = get_chunk_list(tc, symbol_id, type);
		if (list != NULL) {
			struct trcache_trade_data *array = NULL;
			int count = 0;

			/* Consume and apply all trades available for this cursor. */
			while (trade_data_buffer_peek(buf, cur, &array, &count)
					&& count > 0) {
				for (int i = 0; i < count; i++) {
					candle_chunk_list_apply_trade(list, &array[i]);
				}

				trade_data_buffer_consume(buf, cur, count);
			}
		}

		trade_data_buffer_release_cursor(cur);
	}
}

/**
 * @brief   Push a single trade into the internal pipeline.
 *
 * @param   tc:        Pointer to trcache instance.
 * @param   data:      Pointer to trade data struct.
 * @param   symbol_id: Symbol ID of trade data.
 *
 * @return  0 on success, -1 on error.
 *
 * XXX Currently, it is assumed that no more than one user thread receives trade
 * data for a given symbol. If multiple users push trade data for the same
 * symbol concurrently, the implementation must be modified accordingly.
 */
int trcache_feed_trade_data(struct trcache *tc,
	struct trcache_trade_data *data, int symbol_id)
{
	struct trcache_tls_data *tls_data_ptr = get_tls_data_or_create(tc);
	struct trade_data_buffer *trd_databuf;
	struct symbol_entry *symbol_entry;
	bool found = false;

	if (data == NULL || tls_data_ptr == NULL) {
		errmsg(stderr, "Invalid #trcache_trade_data of tls_data_ptr\n");
		return -1;
	}

	/* Initial state */
	if (tls_data_ptr->local_trd_databuf_map == NULL) {
		tls_data_ptr->local_trd_databuf_map
			= ht_create(128, 0, NULL, NULL, NULL, NULL);

		if (tls_data_ptr->local_trd_databuf_map == NULL) {
			errmsg(stderr, "Failure on ht_create()\n");
			return -1;
		}
	}

	trd_databuf = (struct trade_data_buffer *) ht_find(
		tls_data_ptr->local_trd_databuf_map, (void *)(uintptr_t)symbol_id,
		sizeof(void *), &found);

	if (!found) {
		symbol_entry = symbol_table_lookup_entry(tc->symbol_table, symbol_id);
		if (symbol_entry == NULL) {
			errmsg(stderr, "Invalid symbol id\n");
			return -1;
		}

		trd_databuf = symbol_entry->trd_buf;
		
		/* Insert it to the local hash table */
		if (ht_insert(tls_data_ptr->local_trd_databuf_map,
				(void *)(uintptr_t)symbol_id, sizeof(void *),
				trd_databuf) < 0) {
			errmsg(stderr, "Failure on ht_insert()\n");
			return -1;
		}
	}

	/*
	 * If we need free chunk, reap it from data buffer.
	 */
	if (trd_databuf->next_tail_write_idx == NUM_TRADE_CHUNK_CAP - 1 &&
		list_empty(&tls_data_ptr->local_free_list)) {
		trade_data_buffer_reap_free_chunks(trd_databuf,
			&tls_data_ptr->local_free_list);
	}

	/*
	 * Push the trade record into the buffer.
	 */
	if (trade_data_buffer_push(trd_databuf, data,
			&tls_data_ptr->local_free_list) == -1) {
		return -1;
	}

	/* 
	 * Opportunistically apply buffered trades for this symbol.
	 */
	maybe_apply_trades(tc, trd_databuf, symbol_id, data);

	return 0;
}

/**
 * @brief   Copy @p count candles ending at @p ts_end.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   type:       Candle type to query.
 * @param   field_mask: Bitmask of desired candle fields.
 * @param   ts_end:     Timestamp belonging to the last candle.
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_id_and_ts(struct trcache *tc,
	int symbol_id, trcache_candle_type type,
	trcache_candle_field_flags field_mask, uint64_t ts_end, int count,
	struct trcache_candle_batch *dst)
{
	struct candle_chunk_list *list = get_chunk_list(tc, symbol_id, type);

	if (list == NULL) {
		return -1;
	}

	dst->symbol_id = symbol_id;
	dst->candle_type = type;

	return candle_chunk_list_copy_backward_by_ts(list, ts_end, count, 
		dst, field_mask);
}

/**
 * @brief   Copy @p count candles ending at @p ts_end for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   type:       Candle type to query.
 * @param   field_mask: Bitmask of desired candle fields.
 * @param   ts_end:     Timestamp belonging to the last candle.
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_ts(struct trcache *tc,
	const char *symbol_str, trcache_candle_type type,
	trcache_candle_field_flags field_mask, uint64_t ts_end, int count,
	struct trcache_candle_batch *dst)
{
	struct trcache_tls_data *tls = get_tls_data_or_create(tc);
	int symbol_id;

	if (tls == NULL) {
		return -1;
	}

	symbol_id = resolve_symbol_id(tc, tls, symbol_str);
	if (symbol_id == -1) {
		errmsg(stderr, "Invalid symbol string\n");
		return -1;
	}

	return trcache_get_candles_by_symbol_id_and_ts(tc, symbol_id, type, 
		field_mask, ts_end, count, dst);
}

/**
 * @brief   Copy @p count candles ending at the candle located @p offset from
 *          the most recent candle.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   type:       Candle type to query.
 * @param   field_mask: Bitmask of desired candle fields.
 * @param   offset:     Offset from the most recent candle (0 == most recent).
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_id_and_offset(struct trcache *tc,
	int symbol_id, trcache_candle_type type,
	trcache_candle_field_flags field_mask, int offset, int count,
	struct trcache_candle_batch *dst)
{
	struct candle_chunk_list *list = get_chunk_list(tc, symbol_id, type);
	uint64_t seq_end;

	if (list == NULL) {
		return -1;
	}

	seq_end = atomic_load_explicit(&list->mutable_seq, memory_order_acquire);
	seq_end -= offset;

	dst->symbol_id = symbol_id;
	dst->candle_type = type;

	return candle_chunk_list_copy_backward_by_seq(list, seq_end, count, 
		dst, field_mask);
}

/**
 * @brief   Copy @p count candles ending at the candle located @p offset from
 *          the most recent candle for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   type:       Candle type to query.
 * @param   field_mask: Bitmask of desired candle fields.
 * @param   offset:     Offset from the most recent candle (0 == most recent).
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_offset(struct trcache *tc,
	const char *symbol_str, trcache_candle_type type,
	trcache_candle_field_flags field_mask, int offset, int count,
	struct trcache_candle_batch *dst)
{
	struct trcache_tls_data *tls = get_tls_data_or_create(tc);
	int symbol_id;

	if (tls == NULL) {
		return -1;
	}

	symbol_id = resolve_symbol_id(tc, tls, symbol_str);
	if (symbol_id == -1) {
		errmsg(stderr, "Invalid symbol string\n");
		return -1;
	}

	return trcache_get_candles_by_symbol_id_and_offset(tc, symbol_id, type,
		field_mask, offset, count, dst);
}

/**
 * @brief   Print current worker distribution per pipeline stage.
 *
 * Gathers pipeline statistics and computes how many workers the admin
 * scheduler would allocate to each stage. The ranges are printed to stdout.
 *
 * @param   tc: Handle from trcache_init().
 */
void trcache_print_worker_distribution(struct trcache *tc)
{
	int limits[WORKER_STAT_STAGE_NUM];
	int start[WORKER_STAT_STAGE_NUM];
	double speed[WORKER_STAT_STAGE_NUM] = { 0.0, };
	double demand[WORKER_STAT_STAGE_NUM] = { 0.0, };
	int end, idx;

	if (!tc) {
		return;
	}

	update_all_pipeline_stats(tc);

	{
		double hz = tsc_cycles_per_sec();
		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			uint64_t cycles = 0, count = 0;
			for (int w = 0; w < tc->num_workers; w++) {
				struct worker_stat_board *b = &tc->worker_state_arr[w].stat;
				for (uint32_t m = tc->candle_type_flags; m != 0; m &= m - 1) {
					idx = __builtin_ctz(m);
					if (s == WORKER_STAT_STAGE_APPLY) {
						cycles += b->apply_stat[idx].cycles;
						count  += b->apply_stat[idx].work_count;
					} else if (s == WORKER_STAT_STAGE_CONVERT) {
						cycles += b->convert_stat[idx].cycles;
						count  += b->convert_stat[idx].work_count;
					} else {
						cycles += b->flush_stat[idx].cycles;
						count  += b->flush_stat[idx].work_count;
					}
				}
			}
			speed[s] = (cycles != 0) ? ((double)count * hz / (double)cycles) : 0.0;
		}
	}

	{
		struct symbol_table *table = tc->symbol_table;
		struct atomsnap_version *ver
			= atomsnap_acquire_version(table->symbol_ptr_array_gate);
		struct symbol_entry **arr = (struct symbol_entry **)ver->object;
		int num = table->num_symbols;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			demand[s] = 0.0;
		}

		for (int i = 0; i < num; i++) {
			struct symbol_entry *e = arr[i];
			for (uint32_t m = tc->candle_type_flags; m != 0; m &= m - 1) {
				int idx = __builtin_ctz(m);
				struct sched_stage_rate *r = &e->pipeline_stats.stage_rates[idx];
				demand[WORKER_STAT_STAGE_APPLY]   += (double)r->produced_rate;
				demand[WORKER_STAT_STAGE_CONVERT] += (double)r->completed_rate;
				demand[WORKER_STAT_STAGE_FLUSH]   += (double)r->converted_rate;
			}
		}
		atomsnap_release_version(ver);
	}

	compute_stage_limits(tc, limits);
	compute_stage_starts(tc, limits, start);

	printf("Worker distribution and scheduler statistics:\n");
	printf("  Stage speeds (items/s): APPLY=%.2f, CONVERT=%.2f, FLUSH=%.2f\n",
		   speed[WORKER_STAT_STAGE_APPLY],
		   speed[WORKER_STAT_STAGE_CONVERT],
		   speed[WORKER_STAT_STAGE_FLUSH]);
	printf("  Pipeline demand (items/s): APPLY=%.2f, CONVERT=%.2f, FLUSH=%.2f\n",
		   demand[WORKER_STAT_STAGE_APPLY],
		   demand[WORKER_STAT_STAGE_CONVERT],
		   demand[WORKER_STAT_STAGE_FLUSH]);
	printf("  Stage limits: APPLY=%d, CONVERT=%d, FLUSH=%d\n",
		   limits[WORKER_STAT_STAGE_APPLY],
		   limits[WORKER_STAT_STAGE_CONVERT],
		   limits[WORKER_STAT_STAGE_FLUSH]);
	printf("  Stage starts: APPLY=%d, CONVERT=%d, FLUSH=%d\n",
		   start[WORKER_STAT_STAGE_APPLY],
		   start[WORKER_STAT_STAGE_CONVERT],
		   start[WORKER_STAT_STAGE_FLUSH]);

	printf("Worker distribution:\n");
	end = start[WORKER_STAT_STAGE_APPLY] +
		  limits[WORKER_STAT_STAGE_APPLY] - 1;
	printf("  APPLY   : %d..%d\n",
		   start[WORKER_STAT_STAGE_APPLY], end);
	end = start[WORKER_STAT_STAGE_CONVERT] +
		  limits[WORKER_STAT_STAGE_CONVERT] - 1;
	printf("  CONVERT : %d..%d\n",
		   start[WORKER_STAT_STAGE_CONVERT], end);
	end = start[WORKER_STAT_STAGE_FLUSH] +
		  limits[WORKER_STAT_STAGE_FLUSH] - 1;
	printf("  FLUSH   : %d..%d\n",
		   start[WORKER_STAT_STAGE_FLUSH], end);
}

/**
 * @brief   Print a breakdown of the auxiliary memory usage of a trcache.
 *
 * @param   cache: Pointer to a trcache instance as returned from trcache_init().
 */
void trcache_print_aux_memory_breakdown(struct trcache *cache)
{
	if (cache == NULL) {
		return;
	}

	struct memstat *ms = &cache->mem_acc.ms;
	memstat_errmsg_status(ms, true);

	size_t total = memstat_get_aux_total(ms);
	size_t limit = cache->mem_acc.aux_limit;
	if (limit > 0) {
		double pct = (double)total * 100.0 / (double)limit;
		errmsg(stderr,
			"mem_limit=%zu bytes, used=%zu bytes (%.2f%% of limit)\n",
			limit, total, pct);
	} else {
		errmsg(stderr,
			"mem_limit=unlimited, used=%zu bytes\n",
			total);
	}
}

/**
 * @brief   Print a breakdown of the total memory usage of a trcache.
 *
 * @param   cache: Pointer to a trcache instance as returned from trcache_init().
 */
void trcache_print_total_memory_breakdown(struct trcache *cache)
{
	if (cache == NULL) {
		return;
	}

	struct memstat *ms = &cache->mem_acc.ms;
	memstat_errmsg_status(ms, false);
}
