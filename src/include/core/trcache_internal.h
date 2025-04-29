#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

#include <stddef.h>
#include <pthread.h>

#include "core/symbol_table.h"
#include "utils/hash_table.h"

#include "trcache.h"

#define MAX_NUM_THREADS (1024)

typedef struct trcache trcache;

struct trcache_tls_data {
	struct ht_hash_table *local_symbol_id_map;
	struct trcache *trcache_ptr;
	int thread_id;
};

struct trcache {
	pthread_key_t pthread_trcache_key;
	pthread_mutex_t tls_id_mutex;
	_Atomic int tls_id_assigned_flag[MAX_NUM_THREADS];
	_Atomic struct trcache_tls_data *tls_data_ptr_arr[MAX_NUM_THREADS];
	trcache_candle_type_flags candle_type_flags;
	int num_workers;
	int flush_threshold;
	struct symbol_table *symbol_table;
};

#endif /* TRCACHE_INTERNAL_H */
