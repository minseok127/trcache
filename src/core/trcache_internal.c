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

#include "core/trcache_internal.h"
#include "utils/hash_table_callbacks.h"
#include "utils/log.h"

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
	tls_data_ptr->local_trd_databuf_vec = NULL;
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
	struct trade_data_buffer *buf = NULL;
	struct trade_data_chunk *chunk = NULL;
	struct list_head *c = NULL, *n = NULL;

	if (tls_data->local_symbol_id_map != NULL) {
		ht_destroy(tls_data->local_symbol_id_map);
	}

	if (tls_data->local_trd_databuf_map != NULL) {
		ht_destroy(tls_data->local_trd_databuf_map);
	}

	if (tls_data->local_trd_databuf_vec != NULL) {
		for (size_t i = 0; i < tls_data->local_trd_databuf_vec->size; i++) {
			buf = (struct trade_data_buffer *) vector_at(
				tls_data->local_trd_databuf_vec, i);
			trade_data_buffer_destroy(buf);
		}

		vector_destroy(tls_data->local_trd_databuf_vec);
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

	tc->num_candle_types = trcache_candle_type_count(ctx->candle_type_flags);
	tc->num_workers = ctx->num_worker_threads;
	tc->candle_type_flags = ctx->candle_type_flags;
	tc->batch_candle_count_pow2 = ctx->batch_candle_count_pow2;
	tc->batch_candle_count = (1 << ctx->batch_candle_count_pow2);
	tc->flush_threshold_pow2 = ctx->flush_threshold_pow2;
	tc->flush_threshold = (1 << ctx->flush_threshold_pow2);
	tc->flush_ops = ctx->flush_ops;

	pthread_mutex_init(&tc->tls_id_mutex, NULL);

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

	/* Return back trcache id */
	pthread_key_delete(tc->pthread_trcache_key);

	for (int i = 0; i < MAX_NUM_THREADS; i++) {
		if (tc->tls_data_ptr_arr[i] != NULL) {
			destroy_tls_data(tc->tls_data_ptr_arr[i]);
		}
	}

	pthread_mutex_destroy(&tc->tls_id_mutex);

	symbol_table_destroy(tc->symbol_table);

	free(tc);
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
	bool found = false;
	int symbol_id = -1;

	if (tls_data_ptr->local_symbol_id_map == NULL) {
		tls_data_ptr->local_symbol_id_map
			= ht_create(1024, /* initial capacity */
				0xDEADBEEFULL, /* seed */
				murmur_hash, /* hash function */
				compare_symbol_str, /* cmp_func */
				duplicate_symbol_str, /* dup_func */
				free_symbol_str /* free_func */
			);
	}

	/* First, find it from the thread local cache */
	symbol_id = (int)(uintptr_t) ht_find(tls_data_ptr->local_symbol_id_map,
		symbol_str,
		strlen(symbol_str) + 1, /* string + NULL */
		&found);

	if (found) {
		return symbol_id;
	}

	/* 
	 * If this symbol does not exists in local cache, find it from shared
	 * symbol table.
	 */
	symbol_id = symbol_table_lookup_symbol_id(tc->symbol_table, symbol_str);

	if (symbol_id == -1) {
		symbol_id = symbol_table_register(tc->symbol_table, symbol_str);

		/* Add it into local cache */
		if (ht_insert(tls_data_ptr->local_symbol_id_map, symbol_str,
				strlen(symbol_str) + 1, (void *)(uintptr_t)symbol_id) < 0) {
			errmsg(stderr, "Failure on ht_insert()\n");
			return -1;
		}
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
	struct public_symbol_entry *pub_symbol_entry
		= symbol_table_lookup_public_entry(tc->symbol_table, symbol_id);

	if (pub_symbol_entry == NULL) {
		return NULL;
	}

	return pub_symbol_entry->symbol_str;
}

/**
 * @brief   Push a single trade into the internal pipeline.
 *
 * @param   tc:        Pointer to trcache instance.
 * @param   data:      Pointer to trade data struct.
 * @param   symbol_id: Symbol ID of trade data.
 *
 * @return  0 on success, -1 on error.
 */
int trcache_feed_trade_data(struct trcache *tc,
	struct trcache_trade_data *data, int symbol_id)
{
	struct trcache_tls_data *tls_data_ptr = get_tls_data_or_create(tc);
	struct trade_data_buffer *trd_databuf = NULL, *buf = NULL;
	bool found = false;

	if (data == NULL || tls_data_ptr == NULL) {
		errmsg(stderr, "Invalid #trcache_trade_data of tls_data_ptr\n");
		return -1;
	}

	/* Initial state */
	if (tls_data_ptr->local_trd_databuf_map == NULL) {
		tls_data_ptr->local_trd_databuf_map /* simple hash table */
			= ht_create(128, 0, NULL, NULL, NULL, NULL);

		if (tls_data_ptr->local_trd_databuf_map == NULL) {
			errmsg(stderr, "Failure on ht_create()\n");
			return -1;
		}

		tls_data_ptr->local_trd_databuf_vec = vector_init(sizeof(void *));

		if (tls_data_ptr->local_trd_databuf_vec == NULL) {
			ht_destroy(tls_data_ptr->local_trd_databuf_map);
			tls_data_ptr->local_trd_databuf_map = NULL;
			errmsg(stderr, "Failure on vector_init()\n");
			return -1;
		}
	}

	trd_databuf = (struct trade_data_buffer *) ht_find(
		tls_data_ptr->local_trd_databuf_map, (void *)(uintptr_t)symbol_id,
		sizeof(void *), &found);

	if (!found) {
		trd_databuf = trade_data_buffer_init(tc->num_candle_types);

		if (trd_databuf == NULL) {
			errmsg(stderr, "Failure on trade_data_buffer_init()\n");
			return -1;
		}
		
		/* Insert it to the hash table */
		if (ht_insert(tls_data_ptr->local_trd_databuf_map,
				(void *)(uintptr_t)symbol_id, sizeof(void *),
				trd_databuf) < 0) {
			trade_data_buffer_destroy(trd_databuf);
			errmsg(stderr, "Failure on ht_insert()\n");
			return -1;
		}

		/* Add it to the vector */
		if (vector_push_back(tls_data_ptr->local_trd_databuf_vec,
				trd_databuf) < 0) {
			trade_data_buffer_destroy(trd_databuf);
			errmsg(stderr, "Failure on vector_push_back()\n");
			return -1;
		}
	}

	/*
	 * If we need free chunk, reap it from data buffers.
	 */
	if (trd_databuf->next_tail_write_idx == NUM_TRADE_CHUNK_CAP - 1 &&
		list_empty(&tls_data_ptr->local_free_list)) {
		for (size_t i = 0; i < tls_data_ptr->local_trd_databuf_vec->size; i++) {
			buf = (struct trade_data_buffer *) vector_at(
				tls_data_ptr->local_trd_databuf_vec, i);

			trade_data_buffer_reap_free_chunks(buf,
				&tls_data_ptr->local_free_list);

			if (!list_empty(&tls_data_ptr->local_free_list)) {
				break;
			}
		}
	}

	return trade_data_buffer_push(trd_databuf, data, 
		&tls_data_ptr->local_free_list);
}
