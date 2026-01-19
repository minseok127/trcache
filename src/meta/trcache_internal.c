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

#include "concurrent/scalable_queue.h"
#include "meta/trcache_internal.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/hash_table_callbacks.h"
#include "utils/log.h"
#include "utils/tsc_clock.h"
#include "sched/admin_thread.h"

#include "trcache.h"

static size_t align_up(size_t x, size_t a)
{
	return (x + a - 1) & ~(a - 1);
}

/**
 * @brief   Helper to calculate the memory size of a single chunk + batch.
 */
static size_t calculate_chunk_plus_batch_size(
	const struct trcache_candle_config *config, int batch_capacity)
{
	size_t total_size = 0;
	size_t batch_size = 0;

	/* Size of candle_chunk struct */
	total_size = align_up(sizeof(struct candle_chunk), CACHE_LINE_SIZE);

	/* Size of trcache_candle_batch struct */
	batch_size = align_up(sizeof(struct trcache_candle_batch),
		TRCACHE_SIMD_ALIGN);
	/* Size of column_arrays pointer array */
	batch_size = align_up(
		batch_size + sizeof(void *) * config->num_fields,
		TRCACHE_SIMD_ALIGN);
	/* Size of key_array */
	batch_size = align_up(
		batch_size + (size_t)batch_capacity * sizeof(uint64_t),
		TRCACHE_SIMD_ALIGN);
	/* Size of is_closed_array */
	batch_size = align_up(
		batch_size + (size_t)batch_capacity * sizeof(bool),
		TRCACHE_SIMD_ALIGN);

	/* Size of user-defined field columns */
	for (int i = 0; i < config->num_fields; i++) {
		batch_size = align_up(batch_size +
			(size_t)batch_capacity * config->field_definitions[i].size,
			TRCACHE_SIMD_ALIGN);
	}

	return total_size + batch_size;
}

/**
 * @brief   Calculates the minimum required memory for trcache to function.
 */
static size_t calculate_minimum_memory(const struct trcache_init_ctx *ctx)
{
	size_t min_mem = 0;
	size_t min_chunks_per_list = (size_t)(1 << ctx->cached_batch_count_pow2);
	size_t candles_per_chunk = (size_t)(1 << ctx->batch_candle_count_pow2);

	for (int i = 0; i < ctx->num_candle_configs; i++) {
		const struct trcache_candle_config *config = &ctx->candle_configs[i];
		
		/* Calculate size of one chunk + its batch */
		size_t chunk_batch_size = calculate_chunk_plus_batch_size(
			config, candles_per_chunk);

		/* Calculate size of one row page */
		size_t row_page_size = sizeof(struct candle_row_page) +
			(TRCACHE_ROWS_PER_PAGE * config->user_candle_size);
		row_page_size = align_up(row_page_size, TRCACHE_SIMD_ALIGN);

		/* Calculate size of all row pages needed for one chunk */
		size_t pages_per_chunk = (candles_per_chunk + TRCACHE_ROWS_PER_PAGE - 1)
			/ TRCACHE_ROWS_PER_PAGE;
		
		/*
		 * Min memory for one symbol for this candle type:
		 * (N chunks * (chunk_struct + batch_data)) +
		 * (N chunks * M pages_per_chunk * (page_struct + page_data))
		 */
		size_t mem_per_symbol =
			(min_chunks_per_list * chunk_batch_size) +
			(min_chunks_per_list * pages_per_chunk * row_page_size);

		min_mem += mem_per_symbol * (size_t)ctx->max_symbols;
	}

	return min_mem;
}

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
	struct trcache *tc;
	_Atomic(size_t) *free_list_mem_counter;
	size_t chunk_size = sizeof(struct trade_data_chunk);
	size_t total_freed = 0;

	if (tls_data == NULL) {
		return;
	}

	tc = tls_data->trcache_ptr;
	free_list_mem_counter =
		&tc->mem_acc.feed_thread_free_list_mem[tls_data->thread_id].value;

	if (tls_data->local_symbol_id_map != NULL) {
		ht_destroy(tls_data->local_symbol_id_map);
		tls_data->local_symbol_id_map = NULL;
	}

	if (!list_empty(&tls_data->local_free_list)) {
		c = list_get_first(&tls_data->local_free_list);
		while (c != &tls_data->local_free_list) {
			n = c->next;
			chunk = __get_trd_chunk_ptr(c);
			total_freed += chunk_size;
			free(chunk);
			c = n;
		}
	}

	/*
	 * Subtract all freed memory from the thread-local counter in one go.
	 * This must be atomic as the admin thread reads this counter.
	 */
	if (total_freed > 0) {
		mem_sub_atomic(free_list_mem_counter, total_freed);
		assert(*free_list_mem_counter == 0);
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
	struct trcache *tc;
	int thread_id;

	if (tls_data_ptr == NULL) {
		return;
	}

	tc = tls_data_ptr->trcache_ptr;
	thread_id = tls_data_ptr->thread_id;

	/*
	 * Unregister from all SCQ pools. This dumps any locally
	 * cached SCQ nodes back to the shared lists.
	 * This is necessary for *any* thread (feed, reader) that
	 * might have used an SCQ.
	 */
	if (tc->head_version_pool != NULL) {
		scq_thread_unregister(tc->head_version_pool);
	}
	for (int i = 0; i < tc->num_candle_configs; i++) {
		if (tc->chunk_pools[i] != NULL) {
			scq_thread_unregister(tc->chunk_pools[i]);
		}
		if (tc->row_page_pools[i] != NULL) {
			scq_thread_unregister(tc->row_page_pools[i]);
		}
	}

	/* Give back the thread id into the trcache */
	pthread_mutex_lock(&tc->tls_id_mutex);
	atomic_store(&tc->tls_id_assigned_flag[tls_data_ptr->thread_id], 0);
	atomic_store(&tc->tls_data_ptr_arr[tls_data_ptr->thread_id], NULL);

	destroy_tls_data(tls_data_ptr);
	assert(tc->mem_acc.feed_thread_free_list_mem[thread_id].value == 0);
	pthread_mutex_unlock(&tc->tls_id_mutex);
}

#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))

/**
 * @brief   Initialize trcache, set up TLS key and symbol table.
 *
 * @param   ctx: Pointer to a fully-initialised #trcache_init_ctx.
 *
 * @return  Pointer to new trcache, or NULL on failure.
 */
struct trcache *trcache_init(const struct trcache_init_ctx *ctx)
{
	struct trcache *tc = aligned_alloc(CACHE_LINE_SIZE, 
		ALIGN_UP(sizeof(struct trcache), CACHE_LINE_SIZE));
	size_t min_required_memory;
	int ret;

	if (tc == NULL) {
		errmsg(stderr, "#trcache allocation failed\n");
		return NULL;
	}

	memset(tc, 0, sizeof(struct trcache));
	atomic_init(&tc->mem_acc.total_usage.value, 0);
	atomic_init(&tc->mem_acc.memory_pressure, false);

	/* Validate ctx input first */
	if (ctx == NULL || ctx->max_symbols <= 0 || ctx->num_worker_threads <= 2) {
		errmsg(stderr, "trcache_init_ctx is NULL\n");
		free(tc);
		return NULL;
	} else if (ctx->max_symbols <= 0) {
		errmsg(stderr, "Invalid max_symbols\n");
		free(tc);
		return NULL;
	} else if (ctx->num_worker_threads <= 2) {
		errmsg(stderr, "num_worker_threads is less than 2\n");
		free(tc);
		return NULL;
	}

	/*
	 * 1. Calculate and validate memory limits
	 */
	tc->total_memory_limit = ctx->total_memory_limit;
	min_required_memory = calculate_minimum_memory(ctx);

	/*
	 * Require at least the minimum memory to provide buffer space
	 * for auxiliary structures and operational headroom.
	 */
	if (tc->total_memory_limit < min_required_memory) {
		errmsg(stderr, "[trcache_init] Failed: total_memory_limit (%.1f MB) "
			"is less than the minimum required memory (%.1f MB).\n",
			(double)tc->total_memory_limit / (1024.0 * 1024.0),
			(double)(min_required_memory) / (1024.0 * 1024.0));
		
		errmsg(stderr, "[trcache_init] Global settings:\n"
			" - max_symbols: %d\n"
			" - cached_batches_per_list: %d (1 << %d)\n"
			" - candles_per_batch: %d (1 << %d)\n",
			ctx->max_symbols,
			(1 << ctx->cached_batch_count_pow2),
			ctx->cached_batch_count_pow2,
			(1 << ctx->batch_candle_count_pow2),
			ctx->batch_candle_count_pow2);

		errmsg(stderr,
			"[trcache_init] Per-type memory breakdown (for 1 symbol):\n");
		size_t candles_per_chunk
			= (size_t)(1 << ctx->batch_candle_count_pow2);
		size_t min_chunks_per_list
			= (size_t)(1 << ctx->cached_batch_count_pow2);
		
		for (int i = 0; i < ctx->num_candle_configs; i++) {
			const struct trcache_candle_config *config
				= &ctx->candle_configs[i];
			
			size_t chunk_batch_size = calculate_chunk_plus_batch_size(
				config, candles_per_chunk);
			
			size_t row_page_size = sizeof(struct candle_row_page) +
				(TRCACHE_ROWS_PER_PAGE * config->user_candle_size);
			row_page_size = align_up(row_page_size, TRCACHE_SIMD_ALIGN);
			
			size_t pages_per_chunk
				= (candles_per_chunk + TRCACHE_ROWS_PER_PAGE - 1)
					/ TRCACHE_ROWS_PER_PAGE;
			
			size_t mem_per_symbol =
				(min_chunks_per_list * chunk_batch_size) +
				(min_chunks_per_list * pages_per_chunk * row_page_size);

			errmsg(stderr,
				" - Candle Type[%d]: user_candle_size = %zu bytes\n"
				"   -> 1 Chunk (Batch + Struct): %.2f KB\n"
				"   -> 1 Chunk (Row Pages): %.2f KB (%zu pages * %.0f bytes)\n"
				"   -> Min per Symbol (x%zu chunks): %.2f KB\n",
				i, config->user_candle_size,
				(double)chunk_batch_size / 1024.0,
				(double)(pages_per_chunk * row_page_size) / 1024.0,
				pages_per_chunk, (double)row_page_size,
				min_chunks_per_list, (double)mem_per_symbol / 1024.0);
		}

		errmsg(stderr,
			"[trcache_init] Suggestion: Increase total_memory_limit, or "
			"decrease max_symbols or cached_batch_count_pow2.\n");
		
		free(tc);
		return NULL;
	}

	/* 2. Create TLS key with destructor */
	ret = pthread_key_create(&tc->pthread_trcache_key,
		trcache_per_thread_destructor);
	if (ret != 0) {
		errmsg(stderr, "Failure on pthread_key_create()\n");
		free(tc);
		return NULL;
	}

	/* 3. Copy configuration values */
	tc->num_candle_configs = ctx->num_candle_configs;
	tc->num_workers = ctx->num_worker_threads;
	tc->batch_candle_count_pow2 = ctx->batch_candle_count_pow2;
	tc->batch_candle_count = (1 << ctx->batch_candle_count_pow2);
	tc->flush_threshold_pow2 = ctx->cached_batch_count_pow2;
	tc->flush_threshold = (1 << ctx->cached_batch_count_pow2);
	tc->max_symbols = ctx->max_symbols;

	/* 4. Initialize SCQ pools */
	tc->head_version_pool = scq_init(tc);
	if (tc->head_version_pool == NULL) {
		errmsg(stderr, "Failed to initialize head_version_pool\n");
		goto cleanup_key;
	}

	for (int i = 0; i < tc->num_candle_configs; i++) {
		tc->chunk_pools[i] = scq_init(tc);
		if (tc->chunk_pools[i] == NULL) {
			errmsg(stderr, "Failed to initialize chunk_pool %d\n", i);
			goto cleanup_scq_pools;
		}
		tc->row_page_pools[i] = scq_init(tc);
		if (tc->row_page_pools[i] == NULL) {
			errmsg(stderr, "Failed to initialize row_page_pool %d\n", i);
			scq_destroy(tc->chunk_pools[i]);
			goto cleanup_scq_pools;
		}
	}

	/* 5. Initialize shared symbol table */
	tc->symbol_table = symbol_table_init(tc->max_symbols,
		tc->num_candle_configs);
	if (tc->symbol_table == NULL) {
		errmsg(stderr, "Failure on symbol_table_init()\n");
		goto cleanup_scq_pools;
	}

	if (ctx->num_candle_configs > MAX_CANDLE_TYPES) {
		errmsg(stderr, "Too many candle configs. MAX is %d\n",
			MAX_CANDLE_TYPES);
		goto cleanup_symbol_table;
	}

	/* 6. Copy candle configurations (deep copy field definitions) */
	if (ctx->candle_configs && tc->num_candle_configs > 0) {
		size_t configs_size
			= sizeof(trcache_candle_config) * tc->num_candle_configs;
		tc->candle_configs = malloc(configs_size);
		if (tc->candle_configs == NULL) {
			errmsg(stderr, "Failed to allocate memory for candle configs\n");
			goto cleanup_symbol_table;
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
					goto cleanup_symbol_table;
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
		goto cleanup_symbol_table;
	}

	/* 7. Initialize TLS management mutex */
	if (pthread_mutex_init(&tc->tls_id_mutex, NULL) != 0) {
		errmsg(stderr, "Failed to initialize TLS ID mutex\n");
		goto cleanup_candle_configs;
	}

	/* 8. Initialize admin state */
	if (admin_state_init(tc) != 0) {
		errmsg(stderr, "admin_state_init failed\n");
		goto cleanup_tls_mutex;
	}

	/* 9. Allocate worker states */
	tc->worker_state_arr = calloc(tc->num_workers, sizeof(struct worker_state));
	if (tc->worker_state_arr == NULL) {
		errmsg(stderr, "worker_state_arr allocation failed\n");
		goto cleanup_admin_state;
	}

	/* 10. Initialize worker states */
	for (int i = 0; i < tc->num_workers; i++) {
		if (worker_state_init(tc, i) != 0) {
			errmsg(stderr, "worker_state_init failed for worker %d\n", i);
			for (int j = 0; j < i; j++) {
				worker_state_destroy(&tc->worker_state_arr[j]);
			}
			free(tc->worker_state_arr);
			goto cleanup_admin_state;
		}
	}

	/* 11. Allocate worker thread resources */
	tc->worker_threads = calloc(tc->num_workers, sizeof(pthread_t));
	tc->worker_args = calloc(tc->num_workers,
		sizeof(struct worker_thread_args));
	if (tc->worker_threads == NULL || tc->worker_args == NULL) {
		errmsg(stderr, "worker thread resources allocation failed\n");
		goto cleanup_worker_states;
	}

	/* 12. Create worker threads */
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
			goto cleanup_worker_thread_resources;
		}
	}

	/* 13. Create admin thread */
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
		goto cleanup_worker_thread_resources;
	}

	return tc;

	/* Error handling cleanup path */
cleanup_worker_thread_resources:
	free(tc->worker_threads);
	free(tc->worker_args);

cleanup_worker_states:
	for (int i = 0; i < tc->num_workers; i++) {
		worker_state_destroy(&tc->worker_state_arr[i]);
	}
	free(tc->worker_state_arr);

cleanup_admin_state:
	admin_state_destroy(&tc->admin_state);

cleanup_tls_mutex:
	pthread_mutex_destroy(&tc->tls_id_mutex);

cleanup_candle_configs:
	if (tc->candle_configs) {
		for (int i = 0; i < tc->num_candle_configs; i++) {
			free((void *)tc->candle_configs[i].field_definitions);
		}
		free(tc->candle_configs);
	}

cleanup_symbol_table:
	symbol_table_destroy(tc->symbol_table);

cleanup_scq_pools:
	scq_destroy(tc->head_version_pool);
	for (int i = 0; i < tc->num_candle_configs; i++) {
		scq_destroy(tc->chunk_pools[i]);
		scq_destroy(tc->row_page_pools[i]);
	}

cleanup_key:
	pthread_key_delete(tc->pthread_trcache_key);
	free(tc);

	return NULL;
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

	/* 1. Signal threads to stop */
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

	/* 2. Destroy per-thread data that might still exist */
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
			/*
			 * destroy_tls_data will subtract its free list mem
			 * from the counter.
			 */
			destroy_tls_data(tls_data_ptr);
			
			/*
			 * Explicitly zero the counter for this now-dead thread.
			 * We hold the lock, so no new thread can take this ID yet.
			 */
			atomic_store_explicit(
				&tc->mem_acc.feed_thread_free_list_mem[i].value,
				0, memory_order_release);
		}
	}
	pthread_mutex_unlock(&tc->tls_id_mutex);

	/* 3. Delete the TLS key itself */
	pthread_key_delete(tc->pthread_trcache_key);

	/* 4. Destroy remaining shared resources */
	pthread_mutex_destroy(&tc->tls_id_mutex);
	
	/*
	 * Destroy symbol table. This finalizes all candle lists,
	 * flushing data and returning all chunks/pages to the SCQ pools.
	 */
	symbol_table_destroy(tc->symbol_table);
	
	/* Now we can safely destroy the (full) SCQ pools */
	scq_destroy(tc->head_version_pool);
	for (int i = 0; i < tc->num_candle_configs; i++) {
		scq_destroy(tc->chunk_pools[i]);
		scq_destroy(tc->row_page_pools[i]);
	}

	admin_state_destroy(&tc->admin_state);

	for (int i = 0; i < tc->num_workers; i++) {
		worker_state_destroy(&tc->worker_state_arr[i]);
	}

	/* 5. Free arrays */
	free(tc->worker_state_arr);
	free(tc->worker_threads);
	free(tc->worker_args);

	/* 6. Free deep-copied candle configs */
	if (tc->candle_configs) {
		for (int i = 0; i < tc->num_candle_configs; i++) {
			free((void *)tc->candle_configs[i].field_definitions);
		}
		free(tc->candle_configs);
	}

	/* 7. Free the main structure */
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

		/*
		 * Unregister from the pools we *just used* for this candle type
		 * to dump local SCQ node caches.
		 */
		scq_thread_unregister(tc->chunk_pools[i]);
		scq_thread_unregister(tc->row_page_pools[i]);
	}

	scq_thread_unregister(tc->head_version_pool);
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

		is_memory_pressure = atomic_load_explicit(
			&tc->mem_acc.memory_pressure, memory_order_acquire);

		if (is_memory_pressure) {
			user_thread_apply_trades(trd_databuf, symbol_entry);
		}

		trade_data_buffer_reap_free_chunks(trd_databuf,
			&tls_data_ptr->local_free_list, tls_data_ptr->thread_id);
	}

	/*
	 * Push the trade record into the buffer.
	 */
	if (trade_data_buffer_push(trd_databuf, data,
			&tls_data_ptr->local_free_list, tls_data_ptr->thread_id) == -1) {
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
	int ret;

	if (list == NULL) {
		return -1;
	}

	dst->symbol_id = symbol_id;
	dst->candle_idx = candle_idx;

	ret = candle_chunk_list_copy_backward_by_key(list, key, count, 
		dst, request);

	scq_thread_unregister(tc->head_version_pool);
	scq_thread_unregister(tc->row_page_pools[candle_idx]);
	scq_thread_unregister(tc->chunk_pools[candle_idx]);

	return ret;
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
	int ret;

	if (list == NULL) {
		return -1;
	}

	seq_end = atomic_load_explicit(&list->mutable_seq, memory_order_acquire);
	seq_end -= offset;

	dst->symbol_id = symbol_id;
	dst->candle_idx = candle_idx;

	ret = candle_chunk_list_copy_backward_by_seq(list, seq_end, count, 
		dst, request);

	scq_thread_unregister(tc->head_version_pool);
	scq_thread_unregister(tc->row_page_pools[candle_idx]);
	scq_thread_unregister(tc->chunk_pools[candle_idx]);

	return ret;
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
