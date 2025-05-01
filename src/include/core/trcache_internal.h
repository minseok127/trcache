#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

#include <stddef.h>
#include <pthread.h>

#include "core/symbol_table.h"
#include "core/trade_data_buffer.h"
#include "utils/hash_table.h"
#include "utils/list_head.h"
#include "utils/vector.h"

#include "trcache.h"

#define MAX_NUM_THREADS (1024)

typedef struct trcache trcache;

/*
 * trcache_tls_data - Per-thread cache.
 *   
 * @local_symbol_id_map:   Thread-local map from symbol string to ID
 * @local_trd_databuf_map: Thread-local map from symbol ID to trd_databuf
 * @local_trd_databuf_vec: Thread-local vector containing trd_databuf
 * @local_free_list:       Thread-local chunk free-list used by trd_databufs
 * @trcache_ptr:           Back-pointer to owner trcache instance
 * @thread_id:             Assigned index in tls_data_ptr_arr[]
 */
struct trcache_tls_data {
	struct ht_hash_table *local_symbol_id_map;
	struct ht_hash_table *local_trd_databuf_map;
	struct vector *local_trd_databuf_vec;
	struct list_head local_free_list;
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
 * @symbol_table:          Abstracted symbol table (public + admin)
 * @candle_type_flags:     Candle type configuration flags
 * @num_candle_types:      Number of candle types
 * @num_workers:           Number of worker threads
 * @flush_threshold:       Buffer flush threshold
 */
struct trcache {
	pthread_key_t pthread_trcache_key;
	pthread_mutex_t tls_id_mutex;
	_Atomic int tls_id_assigned_flag[MAX_NUM_THREADS];
	_Atomic struct trcache_tls_data *tls_data_ptr_arr[MAX_NUM_THREADS];
	struct symbol_table *symbol_table;
	trcache_candle_type_flags candle_type_flags;
	int num_candle_types;
	int num_workers;
	int flush_threshold;
};

#endif /* TRCACHE_INTERNAL_H */
