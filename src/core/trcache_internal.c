#include <assert.h>
#include <stdio.h>
#include <stdatomic.h>

#include "core/symbol_table.h"
#include "core/trcache_internal.h"

#include "trcache.h"

/*
 * During initialization, the trcache gets an unique ID.
 * This ID is later used when threads access their thread-local data.
 */
static _Atomic int global_trcache_id_spinlock = 0;
static _Atomic int global_trcache_id_array[MAX_TRCACHE_NUM];

static void trcache_id_lock(void)
{
	while (atomic_exchange(&global_trcache_id_spinlock, 1) == 1) {
		__asm__ __volatile__("pause");
	}
}

static void trcache_id_unlock(void)
{
	atomic_store(&global_trcache_id_spinlock, 0);
}

/*
 * Allocate trcache structure and initialize it.
 * Return trcache pointer, or NULL on failure.
 */
struct trcache *trcache_init(int num_worker_threads, int flush_threshold,
	trcache_candle_type_flags candle_type_flags)
{
	struct trcache *tc = calloc(1, sizeof(struct trcache));

	if (tc == NULL) {
		fprintf(stderr, "trcache_init: trcache allocation failed\n");
		return NULL;
	}

	tc->symbol_table = init_symbol_table(1024);
	if (tc->symbol_table == NULL) {
		fprintf(stderr, "trcache_init: init_symbol_table failed\n");
		free(tc);
		return NULL;
	}

	tc->num_workers = num_worker_threads;
	tc->candle_type_flags = candle_type_flags;

	/* Get unique trcache id */
	trcache_id_lock();

	tc->trcache_id = -1;
	for (int i = 0; i < MAX_TRCACHE_NUM; i++) {
		if (atomic_load(&global_trcache_id_array[i]) == 0) {
			atomic_store(&global_trcache_id_array[i], 1);
			tc->trcache_id = i;
			break;
		}
	}

	trcache_id_unlock();

	if (tc->trcache_id == -1) {
		fprintf(stderr, "trcache_init: invalid trcache id\n");
		destroy_symbol_table(tc->symbol_table);
		free(tc);
		return NULL;
	}

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

	assert(tc->trcache_id >= 0 && tc->trcache_id < MAX_TRCACHE_NUM);

	/* Return back trcachd id */
	trcache_id_lock();

	atomic_store(&global_trcache_id_array[tc->trcache_id], 0);

	trcache_id_unlock();

	destroy_symbol_table(tc->symbol_table);

	free(tc);
}

/*
 * Register the given symbol string.
 * Return symbol id (>= 0), or -1 on failure.
 */
int trcache_register_symbol(struct trcache *tc, const char *symbol_str)
{
	int symbol_id = symbol_table_lookup_symbol_id(tc->symbol_table, symbol_str);

	if (symbol_id == -1) {
		symbol_id = symbol_table_register(tc->symbol_table, symbol_str);
	}

	return symbol_id;
}

/*
 * Feed a single trading data into the trcache.
 */
void trcache_feed_trade_data(struct trcache *tc, struct trcache_trade_data *data)
{

}
