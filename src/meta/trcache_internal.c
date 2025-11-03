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
	tls_data_ptr->trcache_ptr = tc;
	tls_data_ptr->thread_id = -1;

	INIT_LIST_HEAD(&tls_data_ptr->local_free_list);

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
	if (pthread_setspecific(tc->pthread_trcache_key,
			(void *)tls_data_ptr) != 0) {
		errmsg(stderr, "Failed to set thread-specific data\n");

		/* Rollback thread ID assignment */
		pthread_mutex_lock(&tc->tls_id_mutex);
		atomic_store(&tc->tls_id_assigned_flag[tls_data_ptr->thread_id], 0);
		atomic_store(&tc->tls_data_ptr_arr[tls_data_ptr->thread_id], NULL);
		pthread_mutex_unlock(&tc->tls_id_mutex);
		free(tls_data_ptr);
		return NULL;
	}

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
	struct memory_accounting *mem_acc;
	struct trcache *tc;

	if (tls_data == NULL) {
		return;
	}

	tc = tls_data->trcache_ptr;
	mem_acc = &tc->mem_acc;

	if (tls_data->local_symbol_id_map != NULL) {
		ht_destroy(tls_data->local_symbol_id_map);
		tls_data->local_symbol_id_map = NULL;
	}

	if (!list_empty(&tls_data->local_free_list)) {
		c = list_get_first(&tls_data->local_free_list);
		while (c != &tls_data->local_free_list) {
			n = c->next;
			chunk = __get_trd_chunk_ptr(c);
			memstat_sub(&mem_acc->ms, MEMSTAT_TRADE_DATA_BUFFER,
				sizeof(struct trade_data_chunk));
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

	if (tls_data_ptr == NULL) {
		return;
	}

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
	struct trcache *tc = aligned_alloc(CACHE_LINE_SIZE, sizeof(struct trcache));
	int ret;

	if (tc == NULL) {
		errmsg(stderr, "#trcache allocation failed\n");
		return NULL;
	}

	memset(tc, 0, sizeof(struct trcache));

	/* Validate ctx input first */
	if (ctx == NULL || ctx->max_symbols <= 0 || ctx->num_worker_threads <= 0) {
		errmsg(stderr, "Invalid arguments in trcache_init_ctx\n");
		free(tc);
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

	/* Copy configuration values */
	tc->num_candle_configs = ctx->num_candle_configs;
	tc->num_workers = ctx->num_worker_threads;
	tc->batch_candle_count_pow2 = ctx->batch_candle_count_pow2;
	tc->batch_candle_count = (1 << ctx->batch_candle_count_pow2);
	tc->flush_threshold_pow2 = ctx->cached_batch_count_pow2;
	tc->flush_threshold = (1 << ctx->cached_batch_count_pow2);
	tc->mem_acc.aux_limit = ctx->aux_memory_limit;
	tc->max_symbols = ctx->max_symbols;

	/* Initialize shared symbol table with fixed capacity */
	tc->symbol_table = symbol_table_init(tc->max_symbols);
	if (tc->symbol_table == NULL) {
		errmsg(stderr, "Failure on symbol_table_init()\n");
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	if (ctx->num_candle_configs > MAX_CANDLE_TYPES) {
		errmsg(stderr, "Too many candle configs. MAX is %d\n",
			MAX_CANDLE_TYPES);
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Copy candle configurations (deep copy field definitions) */
	if (ctx->candle_configs && tc->num_candle_configs > 0) {
		size_t configs_size
			= sizeof(trcache_candle_config) * tc->num_candle_configs;
		tc->candle_configs = malloc(configs_size);
		if (tc->candle_configs == NULL) {
			errmsg(stderr, "Failed to allocate memory for candle configs\n");
			symbol_table_destroy(tc->symbol_table);
			pthread_key_delete(tc->pthread_trcache_key);
			free(tc);
			return NULL;
		}
		memcpy(tc->candle_configs, ctx->candle_configs, configs_size);

		for (int i = 0; i < tc->num_candle_configs; i++) {
			struct trcache_candle_config *config = &tc->candle_configs[i];

			if (config->num_fields > 0 && config->field_definitions != NULL) {
				size_t defs_size
					= sizeof(struct trcache_field_def) * config->num_fields;
				struct trcache_field_def *defs_copy = malloc(defs_size);

				if (defs_copy == NULL) {
					errmsg(stderr,
						"Failed to allocate memory for field def %d\n", i);
					/* Clean up previously allocated definitions */
					for (int j = 0; j < i; j++) {
						free((void *)tc->candle_configs[j].field_definitions);
					}
					free(tc->candle_configs);
					symbol_table_destroy(tc->symbol_table);
					pthread_key_delete(tc->pthread_trcache_key);
					free(tc);
					return NULL;
				}
				memcpy(defs_copy, config->field_definitions, defs_size);
				config->field_definitions = defs_copy; /* Point to the copy */
			} else {
				/* Ensure consistent state even if no fields */
				config->field_definitions = NULL;
				config->num_fields = 0;
			}
		}
	} else {
		errmsg(stderr, "Invalid candle config\n");
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Initialize TLS management mutex */
	if (pthread_mutex_init(&tc->tls_id_mutex, NULL) != 0) {
		errmsg(stderr, "Failed to initialize TLS ID mutex\n");
		/* Cleanup already allocated candle config copies */
		if (tc->candle_configs) {
			for (int i = 0; i < tc->num_candle_configs; i++) {
				free((void *)tc->candle_configs[i].field_definitions);
			}
			free(tc->candle_configs);
		}
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Initialize scheduler message free list */
	tc->sched_msg_free_list = scq_init(&tc->mem_acc);
	if (tc->sched_msg_free_list == NULL) {
		errmsg(stderr, "sched_msg_free_list allocation failed\n");
		pthread_mutex_destroy(&tc->tls_id_mutex);
		if (tc->candle_configs) {
			for (int i = 0; i < tc->num_candle_configs; i++) {
				free((void *)tc->candle_configs[i].field_definitions);
			}
			free(tc->candle_configs);
		}
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Initialize admin state */
	if (admin_state_init(tc) != 0) {
		errmsg(stderr, "admin_state_init failed\n");
		scq_destroy(tc->sched_msg_free_list);
		pthread_mutex_destroy(&tc->tls_id_mutex);
		if (tc->candle_configs) {
			for (int i = 0; i < tc->num_candle_configs; i++) {
				free((void *)tc->candle_configs[i].field_definitions);
			}
			free(tc->candle_configs);
		}
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Allocate worker states */
	tc->worker_state_arr = calloc(tc->num_workers, sizeof(struct worker_state));
	if (tc->worker_state_arr == NULL) {
		errmsg(stderr, "worker_state_arr allocation failed\n");
		admin_state_destroy(&tc->admin_state);
		scq_destroy(tc->sched_msg_free_list);
		pthread_mutex_destroy(&tc->tls_id_mutex);
		if (tc->candle_configs) {
			for (int i = 0; i < tc->num_candle_configs; i++) {
				free((void *)tc->candle_configs[i].field_definitions);
			}
			free(tc->candle_configs);
		}
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Initialize worker states */
	for (int i = 0; i < tc->num_workers; i++) {
		if (worker_state_init(tc, i) != 0) {
			errmsg(stderr, "worker_state_init failed for worker %d\n", i);
			for (int j = 0; j < i; j++) {
				worker_state_destroy(&tc->worker_state_arr[j]);
			}
			free(tc->worker_state_arr);
			admin_state_destroy(&tc->admin_state);
			scq_destroy(tc->sched_msg_free_list);
			pthread_mutex_destroy(&tc->tls_id_mutex);
			if (tc->candle_configs) {
				for (int j = 0; j < tc->num_candle_configs; j++) {
					free((void *)tc->candle_configs[j].field_definitions);
				}
				free(tc->candle_configs);
			}
			symbol_table_destroy(tc->symbol_table);
			pthread_key_delete(tc->pthread_trcache_key);
			free(tc);
			return NULL;
		}
	}

	/* Allocate worker thread resources */
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
		if (tc->candle_configs) {
			for (int i = 0; i < tc->num_candle_configs; i++) {
				free((void *)tc->candle_configs[i].field_definitions);
			}
			free(tc->candle_configs);
		}
		symbol_table_destroy(tc->symbol_table);
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	/* Create worker threads */
	for (int i = 0; i < tc->num_workers; i++) {
		tc->worker_args[i].cache = tc;
		tc->worker_args[i].worker_id = i;

		ret = pthread_create(&tc->worker_threads[i], NULL,
				worker_thread_main, &tc->worker_args[i]);
		if (ret != 0) {
			errmsg(stderr, "Failure on pthread_create() for worker %d: %s\n",
				 i, strerror(ret));
			/* Signal already created threads and admin to stop */
			tc->admin_state.done = true; /* Try to stop admin if created */
			for (int j = 0; j < i; j++) {
				tc->worker_state_arr[j].done = true;
			}
			/* Join threads that were successfully created */
			for (int j = 0; j < i; j++) {
				pthread_join(tc->worker_threads[j], NULL);
			}
			/* Cleanup */
			free(tc->worker_threads);
			free(tc->worker_args);
			for (int j = 0; j < tc->num_workers; j++) { /* Destroy all states */
				worker_state_destroy(&tc->worker_state_arr[j]);
			}
			free(tc->worker_state_arr);
			admin_state_destroy(&tc->admin_state);
			scq_destroy(tc->sched_msg_free_list);
			pthread_mutex_destroy(&tc->tls_id_mutex);
			if (tc->candle_configs) {
				for (int j = 0; j < tc->num_candle_configs; j++) {
					free((void *)tc->candle_configs[j].field_definitions);
				}
				free(tc->candle_configs);
			}
			symbol_table_destroy(tc->symbol_table);
			pthread_key_delete(tc->pthread_trcache_key);
			free(tc);
			return NULL;
		}
	}

	/* Create admin thread */
	ret = pthread_create(&tc->admin_thread, NULL, admin_thread_main, tc);
	if (ret != 0) {
		errmsg(stderr, "Failure on pthread_create() for admin: %s\n",
			strerror(ret));
		/* Signal all worker threads to stop */
		for (int i = 0; i < tc->num_workers; i++) {
			tc->worker_state_arr[i].done = true;
		}
		/* Join all worker threads */
		for (int i = 0; i < tc->num_workers; i++) {
			pthread_join(tc->worker_threads[i], NULL);
		}
		/* Cleanup */
		free(tc->worker_threads);
		free(tc->worker_args);
		for (int i = 0; i < tc->num_workers; i++) {
			worker_state_destroy(&tc->worker_state_arr[i]);
		}
		free(tc->worker_state_arr);
		admin_state_destroy(&tc->admin_state);
		scq_destroy(tc->sched_msg_free_list);
		pthread_mutex_destroy(&tc->tls_id_mutex);
		if (tc->candle_configs) {
			for (int i = 0; i < tc->num_candle_configs; i++) {
				free((void *)tc->candle_configs[i].field_definitions);
			}
			free(tc->candle_configs);
		}
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
	struct trcache_tls_data *tls_data_ptr = NULL;

	if (tc == NULL) {
		return;
	}

	/* Signal threads to stop */
	if (!tc->admin_state.done) {
		tc->admin_state.done = true;
		pthread_join(tc->admin_thread, NULL);
	}

	for (int i = 0; i < tc->num_workers; i++) {
		if (!tc->worker_state_arr[i].done) {
			tc->worker_state_arr[i].done = true;
			pthread_join(tc->worker_threads[i], NULL);
		}
	}

	/* Destroy per-thread data that might still exist */
	pthread_mutex_lock(&tc->tls_id_mutex);
	for (int i = 0; i < MAX_NUM_THREADS; i++) {
		tls_data_ptr = tc->tls_data_ptr_arr[i];
		if (tls_data_ptr != NULL) {
			/* Clear the pointer and mark as unused before destroying */
			tc->tls_data_ptr_arr[i] = NULL;
			tc->tls_id_assigned_flag[i] = 0;

			/* Detach from pthread TLS mechanism *before* destroying */
			if (pthread_getspecific(tc->pthread_trcache_key) == tls_data_ptr) {
				pthread_setspecific(tc->pthread_trcache_key, NULL);
			}
			destroy_tls_data(tls_data_ptr);
		}
	}
	pthread_mutex_unlock(&tc->tls_id_mutex);

	/* Delete the TLS key itself */
	pthread_key_delete(tc->pthread_trcache_key);

	/* Destroy remaining shared resources */
	pthread_mutex_destroy(&tc->tls_id_mutex);
	symbol_table_destroy(tc->symbol_table);
	admin_state_destroy(&tc->admin_state);

	for (int i = 0; i < tc->num_workers; i++) {
		worker_state_destroy(&tc->worker_state_arr[i]);
	}
	scq_destroy(tc->sched_msg_free_list);

	/* Free arrays */
	free(tc->worker_state_arr);
	free(tc->worker_threads);
	free(tc->worker_args);

	/* Free deep-copied candle configs */
	if (tc->candle_configs) {
		for (int i = 0; i < tc->num_candle_configs; i++) {
			free((void *)tc->candle_configs[i].field_definitions);
		}
		free(tc->candle_configs);
	}

	/* Free the main structure */
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
 *
 * Internal static function. Uses thread-local cache first.
 */
static struct candle_chunk_list *get_chunk_list(struct trcache *tc,
	int symbol_id, int candle_idx)
{
	struct symbol_entry *entry = NULL;

	entry = symbol_table_lookup_entry(tc->symbol_table, symbol_id);
	if (entry == NULL) {
		return NULL;
	}

	/* Validate candle_idx */
	if (candle_idx < 0 || candle_idx >= tc->num_candle_configs) {
		errmsg(stderr, "Invalid candle_idx: %d (num_configs: %d)\n",
			candle_idx, tc->num_candle_configs);
		return NULL;
	}

	/* Return the candle chunk list pointer */
	return entry->candle_chunk_list_ptrs[candle_idx];
}

/**
 * @brief   Apply trades from the buffer by the user thread.
 *
 * This function is called by the user thread (producer) when the
 * trade_data_buffer is under memory pressure. It attempts to acquire a cursor
 * for each candle type and process pending trades, thereby freeing up space
 * in the buffer. This helps to alleviate back-pressure without blocking.
 *
 * @param   buf:           Buffer to consume from.
 * @param   symbol_entry:  Target symbol entry.
 */
static void user_thread_apply_trades(struct trade_data_buffer *buf,
	struct symbol_entry *symbol_entry)
{
	struct trade_data_buffer_cursor *cur;
	struct candle_chunk_list *list;
	struct trcache_trade_data *array;
	struct trcache *tc = buf->trc;
	int count;

	for (int i = 0; i < tc->num_candle_configs; i++) {
		list = symbol_entry->candle_chunk_list_ptrs[i];
		if (list == NULL) {
			errmsg(stderr, "Candle chunk list is NULL\n");
			return;
		}

		cur = trade_data_buffer_acquire_cursor(buf, i);

		/* This candle type is being processed by a worker thread. */
		if (cur == NULL) {
			continue;
		}

		while (trade_data_buffer_peek(buf, cur, &array, &count) && count > 0) {
			for (int j = 0; j < count; j++) {
				candle_chunk_list_apply_trade(list, &array[j]);
			}
			trade_data_buffer_consume(buf, cur, count);
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
	bool is_memory_pressure;

	if (data == NULL || tls_data_ptr == NULL) {
		errmsg(stderr, "Invalid #trcache_trade_data of tls_data_ptr\n");
		return -1;
	}

	symbol_entry = symbol_table_lookup_entry(tc->symbol_table, symbol_id);
	if (symbol_entry == NULL) {
		errmsg(stderr, "Symbol id %d not found via TLS or global table\n",
			symbol_id);
		return -1;
	}

	/* Get the trade data buffer from the symbol entry */
	trd_databuf = symbol_entry->trd_buf;
	if (trd_databuf == NULL) {
		errmsg(stderr, "Trade data buffer is NULL for symbol id %d\n",
			symbol_id);
		return -1;
	}

	/* If we need free chunk, reap it from data buffer. */
	if (trd_databuf->next_tail_write_idx == NUM_TRADE_CHUNK_CAP - 1 &&
			list_empty(&tls_data_ptr->local_free_list)) {
		is_memory_pressure = (tc->mem_acc.aux_limit > 0 &&
			memstat_get_aux_total(&tc->mem_acc.ms) > tc->mem_acc.aux_limit);

		if (is_memory_pressure) {
			user_thread_apply_trades(trd_databuf, symbol_entry);
		}

		trade_data_buffer_reap_free_chunks(trd_databuf,
			&tls_data_ptr->local_free_list, is_memory_pressure);
	}

	/*
	 * Push the trade record into the buffer.
	 */
	if (trade_data_buffer_push(trd_databuf, data,
			&tls_data_ptr->local_free_list) == -1) {
		return -1;
	}

	return 0;
}

/**
 * @brief   Copy @count candles ending at the candle with key @key.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query.
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 * @param   key:        Key of the last candle to retrieve.
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_id_and_key(struct trcache *tc,
	int symbol_id, int candle_idx, const struct trcache_field_request *request,
	uint64_t key, int count, struct trcache_candle_batch *dst)
{
	struct candle_chunk_list *list = get_chunk_list(tc, symbol_id, candle_idx);

	if (list == NULL) {
		return -1;
	}

	dst->symbol_id = symbol_id;
	dst->candle_idx = candle_idx;

	return candle_chunk_list_copy_backward_by_key(list, key, count, 
		dst, request);
}

/**
 * @brief   Copy @count candles ending at the candle with key @key
 *          for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   candle_idx: Candle type to query.
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 * @param   key:        Key of the last candle to retrieve.
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_key(struct trcache *tc,
	const char *symbol_str, int candle_idx,
	const struct trcache_field_request *request, uint64_t key, int count,
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

	return trcache_get_candles_by_symbol_id_and_key(tc, symbol_id, candle_idx, 
		request, key, count, dst);
}

/**
 * @brief   Copy @count candles ending at the candle located @offset from
 *          the most recent candle.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_id:  Symbol ID from trcache_register_symbol().
 * @param   candle_idx: Candle type to query.
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 * @param   offset:     Offset from the most recent candle (0 == most recent).
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_id_and_offset(struct trcache *tc,
	int symbol_id, int candle_idx, const struct trcache_field_request *request,
	int offset, int count, struct trcache_candle_batch *dst)
{
	struct candle_chunk_list *list = get_chunk_list(tc, symbol_id, candle_idx);
	uint64_t seq_end;

	if (list == NULL) {
		return -1;
	}

	seq_end = atomic_load_explicit(&list->mutable_seq, memory_order_acquire);
	seq_end -= offset;

	dst->symbol_id = symbol_id;
	dst->candle_idx = candle_idx;

	return candle_chunk_list_copy_backward_by_seq(list, seq_end, count, 
		dst, request);
}

/**
 * @brief   Copy @count candles ending at the candle located @offset from
 *          the most recent candle for a symbol string.
 *
 * @param   tc:         Pointer to trcache instance.
 * @param   symbol_str: NULL-terminated symbol string.
 * @param   candle_idx: Candle type to query.
 * @param   request:    Pointer to a struct specifying which fields to retrieve.
 * @param   offset:     Offset from the most recent candle (0 == most recent).
 * @param   count:      Number of candles to copy.
 * @param   dst:        Pre-allocated destination batch.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_candles_by_symbol_str_and_offset(struct trcache *tc,
	const char *symbol_str, int candle_idx,
	const struct trcache_field_request *request, int offset, int count,
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

	return trcache_get_candles_by_symbol_id_and_offset(tc, symbol_id,
		candle_idx, request, offset, count, dst);
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
	double capacity[WORKER_STAT_STAGE_NUM] = { 0.0, };
	double demand[WORKER_STAT_STAGE_NUM] = { 0.0, };
	struct symbol_entry *e;
	int end;

	if (!tc) {
		return;
	}

	{
		double hz = tsc_cycles_per_sec();

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			uint64_t cycles = 0, count = 0;

			for (int w = 0; w < tc->num_workers; w++) {
				struct worker_stat_board *b = &tc->worker_state_arr[w].stat;

				for (int i = 0; i < tc->num_candle_configs; i++) {
					if (s == WORKER_STAT_STAGE_APPLY) {
						cycles += b->apply_stat[i].cycles;
						count  += b->apply_stat[i].work_count;
					} else if (s == WORKER_STAT_STAGE_CONVERT) {
						cycles += b->convert_stat[i].cycles;
						count  += b->convert_stat[i].work_count;
					} else {
						cycles += b->flush_stat[i].cycles;
						count  += b->flush_stat[i].work_count;
					}
				}
			}

			speed[s] = (cycles != 0) ? ((double)count * hz / (double)cycles) : 0.0;
		}
	}

	{
		struct symbol_table *table = tc->symbol_table;
		int num = table->num_symbols;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			demand[s] = 0.0;
		}

		for (int i = 0; i < num; i++) {
			e = &table->symbol_entries[i];

			for (int j = 0; j < tc->num_candle_configs; j++) {
				struct sched_stage_rate *r
					= &e->pipeline_stats.stage_rates[j];

				demand[WORKER_STAT_STAGE_APPLY]
					+= (double)r->produced_rate;
				demand[WORKER_STAT_STAGE_CONVERT]
					+= (double)r->completed_rate;
				demand[WORKER_STAT_STAGE_FLUSH]
					+= (double)r->flushable_batch_rate;
			}
		}
	}

	compute_stage_limits(tc, limits);
	compute_stage_starts(tc, limits, start);

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		capacity[s] = speed[s] * limits[s];
	}

	printf("Worker distribution and scheduler statistics:\n");
	printf("  Stage speeds (items/s): APPLY=%.2f, CONVERT=%.2f, FLUSH=%.2f\n",
		   speed[WORKER_STAT_STAGE_APPLY],
		   speed[WORKER_STAT_STAGE_CONVERT],
		   speed[WORKER_STAT_STAGE_FLUSH]);
	printf("  Stage Capacity (items/s): APPLY=%.2f, CONVERT=%.2f, FLUSH=%.2f\n",
		   capacity[WORKER_STAT_STAGE_APPLY],
		   capacity[WORKER_STAT_STAGE_CONVERT],
		   capacity[WORKER_STAT_STAGE_FLUSH]);
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

/**
 * @brief   Get the current worker distribution and scheduler statistics.
 *
 * @param   cache:  Handle from trcache_init().
 * @param   stats:  Pointer to a user-allocated struct to be filled.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_worker_distribution(struct trcache *cache,
	trcache_worker_distribution_stats *stats)
{
	struct symbol_entry *e;

	if (!cache || !stats) {
		return -1;
	}

	{
		double hz = tsc_cycles_per_sec();

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			uint64_t cycles = 0, count = 0;

			for (int w = 0; w < cache->num_workers; w++) {
				struct worker_stat_board *b = &cache->worker_state_arr[w].stat;

				for (int i = 0; i < cache->num_candle_configs; i++) {
					if (s == WORKER_STAT_STAGE_APPLY) {
						cycles += b->apply_stat[i].cycles;
						count += b->apply_stat[i].work_count;
					} else if (s == WORKER_STAT_STAGE_CONVERT) {
						cycles += b->convert_stat[i].cycles;
						count += b->convert_stat[i].work_count;
					} else {
						cycles += b->flush_stat[i].cycles;
						count += b->flush_stat[i].work_count;
					}
				}
			}

			stats->stage_speeds[s] = (cycles != 0) ?
				((double)count * hz / (double)cycles) : 0.0;
		}
	}

	{
		struct symbol_table *table = cache->symbol_table;
		int num = table->num_symbols;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			stats->pipeline_demand[s] = 0.0;
		}

		for (int i = 0; i < num; i++) {
			e = &table->symbol_entries[i];

			for (int j = 0; j < cache->num_candle_configs; j++) {
				struct sched_stage_rate *r
					= &e->pipeline_stats.stage_rates[j];

				stats->pipeline_demand[WORKER_STAT_STAGE_APPLY]
					+= (double)r->produced_rate;
				stats->pipeline_demand[WORKER_STAT_STAGE_CONVERT]
					+= (double)r->completed_rate;
				stats->pipeline_demand[WORKER_STAT_STAGE_FLUSH]
					+= (double)r->flushable_batch_rate;
			}
		}
	}

	compute_stage_limits(cache, stats->stage_limits);
	compute_stage_starts(cache, stats->stage_limits, stats->stage_starts);

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		stats->stage_capacity[s] = stats->stage_speeds[s] * stats->stage_limits[s];
	}

	return 0;
}

/**
 * @brief   Get a snapshot of the auxiliary memory usage.
 *
 * @param   cache:  Handle from trcache_init().
 * @param   stats:  Pointer to a user-allocated struct to be filled.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_aux_memory_breakdown(struct trcache *cache,
	trcache_memory_stats *stats)
{
	if (cache == NULL || stats == NULL) {
		return -1;
	}

	struct memstat *ms = &cache->mem_acc.ms;
	for (int i = 0; i < MEMSTAT_CATEGORY_NUM; i++) {
		if (i == MEMSTAT_CANDLE_CHUNK_LIST || i == MEMSTAT_CANDLE_CHUNK_INDEX) {
			stats->usage_bytes[i] = 0;
			continue;
		}
		stats->usage_bytes[i] = memstat_get(ms, (memstat_category)i);
	}

	return 0;
}

/**
 * @brief   Get a snapshot of the total memory usage.
 *
 * @param   cache:  Handle from trcache_init().
 * @param   stats:  Pointer to a user-allocated struct to be filled.
 *
 * @return  0 on success, -1 on failure.
 */
int trcache_get_total_memory_breakdown(struct trcache *cache,
	trcache_memory_stats *stats)
{
	if (cache == NULL || stats == NULL) {
		return -1;
	}

	struct memstat *ms = &cache->mem_acc.ms;
	for (int i = 0; i < MEMSTAT_CATEGORY_NUM; i++) {
		stats->usage_bytes[i] = memstat_get(ms, (memstat_category)i);
	}

	return 0;
}
