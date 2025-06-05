#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

#include <stddef.h>
#include <pthread.h>

#include "meta/symbol_table.h"
#include "pipeline/trade_data_buffer.h"
#include "utils/hash_table.h"
#include "utils/list_head.h"
#include "sched/worker_state.h"
#include "sched/sched_msg.h"

#include "trcache.h"

#define MAX_NUM_THREADS (1024)

typedef struct trcache trcache;

/*
 * trcache_tls_data - Per-thread cache.
 *   
 * @local_symbol_id_map:   Thread-local map from symbol string to ID.
 * @local_trd_databuf_map: Thread-local map from symbol ID to trd_databuf.
 * @local_free_list:       Thread-local chunk free-list used by trd_databufs.
 * @trcache_ptr:           Back-pointer to owner trcache instance.
 * @thread_id:             Assigned index in tls_data_ptr_arr[].
 */
struct trcache_tls_data {
	struct ht_hash_table *local_symbol_id_map;
	struct ht_hash_table *local_trd_databuf_map;
	struct list_head local_free_list;
	struct trcache *trcache_ptr;
	int thread_id;
};

/*
 * trcache - trcache internal state.
 *
 * @pthread_trcache_key:     Key for pthread_get/setspecific.
 * @tls_id_mutex:            Protects allocation/release of thread IDs.
 * @tls_id_assigned_flag:    _Atomic flags, which slots are in use.
 * @tls_data_ptr_arr:        Pointers to each thread’s tls_data.
 * @symbol_table:            Abstracted symbol table.
 * @candle_type_flags:       Candle type configuration flags.
 * @num_candle_types:        Number of candle types.
 * @num_workers:             Number of worker threads.
 * @batch_candle_count:      Number of candles per column batch.
 * @batch_candle_count_pow2: Equal to log2(@batch_candle_count).
 * @flush_threshold:         How many candle batches to buffer before flush.
 * @flush_threshold_pow2:    Equal to log2(@flush_threshold_batches).
 * @flush_ops:               User-supplied callbacks used for flush.
 * @worker_state_arr:        Per-worker state array of length @num_workers.
 * @sched_msg_free_list:     Free list for scheduler message objects.
 */
struct trcache {
	pthread_key_t pthread_trcache_key;
	pthread_mutex_t tls_id_mutex;
	_Atomic int tls_id_assigned_flag[MAX_NUM_THREADS];
	struct trcache_tls_data *tls_data_ptr_arr[MAX_NUM_THREADS];
	struct symbol_table *symbol_table;
	trcache_candle_type_flags candle_type_flags;
	int num_candle_types;
	int num_workers;
	int batch_candle_count;
	int batch_candle_count_pow2;
	int flush_threshold;
	int flush_threshold_pow2;
	struct trcache_flush_ops flush_ops;
	struct worker_state *worker_state_arr;
	sched_msg_free_list *sched_msg_free_list;
};

/**
 * @brief	Count how many candle-type bits are set in @flags.
 *
 * @param	flags:	Bit-OR of ::trcache_candle_type values.
 *
 * @return	Number of enabled types (0...TRCACHE_NUM_CANDLE_TYPE).
 */
static inline int trcache_candle_type_count(
	trcache_candle_type_flags flags)
{
	return __builtin_popcount((unsigned int)flags);
}

#endif /* TRCACHE_INTERNAL_H */
