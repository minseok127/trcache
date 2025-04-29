#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

#include <stddef.h>
#include <pthread.h>

#include "core/symbol_table.h"
#include "utils/hash_table.h"

#include "trcache.h"

#define MAX_NUM_THREADS (1024)

typedef struct trcache trcache;

/*
 * trcache_tls_data - Per-thread cache.
 *   
 * @local_symbol_id_map: Thread-local map from symbol string to ID
 * @trcache_ptr:         Back-pointer to owner trcache instance
 * @thread_id:           Assigned index in tls_data_ptr_arr[]
 */
struct trcache_tls_data {
	struct ht_hash_table *local_symbol_id_map;
	struct trcache *trcache_ptr;
	int thread_id;
};

/*
 * trcache - trcache internal state.
 *
 * @pthread_trcache_key:   Key for pthread_get/setspecific
 * @tls_id_mutex:          Protects allocation/release of thread IDs
 * @tls_id_assigned_flag:  _Atomic flags, which slots are in use
 * @tls_data_ptr_arr:      _Atomic pointers to each thread’s tls_data
 * @candle_type_flags:     Candle type configuration flags
 * @num_workers:           Number of worker threads
 * @flush_threshold:       Buffer flush threshold
 * @symbol_table:          Abstracted symbol table (public + admin)
 */
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
