#include <assert.h>
#include <stdio.h>
#include <stdatomic.h>

#include "core/symbol_table.h"
#include "core/trcache_internal.h"
#include "utils/hash_table_callbacks.h"

#include "trcache.h"

/*
 * Ensures that the per-thread data is initialized and returns it.
 * If the thread-local data is already initialized, returns it directly.
 * Otherwise, initializes the data and then returns it.
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
		fprintf(stderr, "get_tls_data_or_create: malloc failed\n");
		return NULL;
	}

	tls_data_ptr->local_symbol_id_map = NULL;
	tls_data_ptr->trcache_ptr = tc;
	tls_data_ptr->thread_id = -1;

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
		fprintf(stderr, "get_tls_data_or_create: invalid thread id\n");
		free(tls_data_ptr);
		return NULL;
	}

	/* Set argument of trcache_per_thread_destructor() */
	pthread_setspecific(tc->pthread_trcache_key, (void *)tls_data_ptr);

	return tls_data_ptr;
}

/*
 * Function commonly used by both the per-thread destructor and
 * trcache_destroy.
 */
static void destroy_tls_data(struct trcache_tls_data *tls_data)
{
	if (tls_data->local_symbol_id_map != NULL) {
		ht_destroy(tls_data->local_symbol_id_map);
	}
}

/* 
 * Destructor for freeing per-thread data upon thread termination.
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

/*
 * Allocate trcache structure and initialize it.
 * Return trcache pointer, or NULL on failure.
 */
struct trcache *trcache_init(int num_worker_threads, int flush_threshold,
	trcache_candle_type_flags candle_type_flags)
{
	struct trcache *tc = calloc(1, sizeof(struct trcache));
	int ret;

	if (tc == NULL) {
		fprintf(stderr, "trcache_init: trcache allocation failed\n");
		return NULL;
	}

	ret = pthread_key_create(&tc->pthread_trcache_key,
		trcache_per_thread_destructor);
	if (ret != 0) {
		fprintf(stderr, "trcache_init: pthread_key_create failed, erro: %d\n",
			ret);
		free(tc);
		return NULL;
	}

	tc->symbol_table = init_symbol_table(1024);
	if (tc->symbol_table == NULL) {
		fprintf(stderr, "trcache_init: init_symbol_table failed\n");
		pthread_key_delete(tc->pthread_trcache_key);
		free(tc);
		return NULL;
	}

	tc->num_workers = num_worker_threads;
	tc->candle_type_flags = candle_type_flags;
	tc->flush_threshold = flush_threshold;

	pthread_mutex_init(&tc->tls_id_mutex, NULL);

	return tc;
}

/*
 * Destroy all resources.
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

	destroy_symbol_table(tc->symbol_table);

	free(tc);
}

/*
 * Register the given symbol string.
 * Return symbol id (>= 0), or -1 on failure.
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
	symbol_id = (int) ht_find(tls_data_ptr->local_symbol_id_map, symbol_str,
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
				strlen(symbol_str) + 1, (void *)id) < 0) {
			fprintf(stderr, "trcache_register_symbol: local cache insert failed\n");
		}
	}

	return symbol_id;
}

/*
 * Feed a single trading data into the trcache.
 */
void trcache_feed_trade_data(struct trcache *tc, struct trcache_trade_data *data)
{
	struct trcache_tls_data *tls_data_ptr = get_tls_data_or_create(tc);
}
