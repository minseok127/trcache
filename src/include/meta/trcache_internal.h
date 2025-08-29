#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

#include <stddef.h>
#include <pthread.h>

#include "meta/symbol_table.h"
#include "pipeline/trade_data_buffer.h"
#include "utils/hash_table.h"
#include "utils/list_head.h"
#include "utils/memstat.h"
#include "sched/worker_thread.h"
#include "sched/admin_thread.h"
#include "sched/sched_work_msg.h"

#include "trcache.h"

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
 * @candle_configs:          Candle configurations.
 * @num_candle_types:        Number of candle types for each base.
 * @num_workers:             Number of worker threads.
 * @batch_candle_count:      Number of candles per column batch.
 * @batch_candle_count_pow2: Equal to log2(@batch_candle_count).
 * @flush_threshold:         How many candle batches to buffer before flush.
 * @flush_threshold_pow2:    Equal to log2(@flush_threshold_batches).
 * @worker_state_arr:        Per-worker state array of length @num_workers.
 * @stage_ct_mask:           Candle-type ownership mask per stage/worker/base.
 * @admin_state:             State structure for admin thread.
 * @sched_msg_free_list:     Free list for scheduler message objects.
 * @admin_thread:            Handle for admin thread.
 * @worker_threads:          Array of handles for worker threads.
 * @worker_args:             Per-worker argument array used at start.
 * @mem_acc:                 All modules use &mem_acc to update memory usage.
*/
struct trcache {
	pthread_key_t pthread_trcache_key;
	pthread_mutex_t tls_id_mutex;
	_Atomic int tls_id_assigned_flag[MAX_NUM_THREADS];
	struct trcache_tls_data *tls_data_ptr_arr[MAX_NUM_THREADS];
	struct symbol_table *symbol_table;
	trcache_candle_config candle_configs[NUM_CANDLE_BASES][MAX_CANDLE_TYPES_PER_BASE];
	int num_candle_types[NUM_CANDLE_BASES];
	int num_workers;
	int batch_candle_count;
	int batch_candle_count_pow2;
	int flush_threshold;
	int flush_threshold_pow2;
	struct worker_state *worker_state_arr;
	uint32_t stage_ct_mask[WORKER_STAT_STAGE_NUM][MAX_NUM_THREADS][NUM_CANDLE_BASES];
	struct admin_state admin_state;
	sched_work_msg_free_list *sched_msg_free_list;
	pthread_t admin_thread;
	pthread_t *worker_threads;
	struct worker_thread_args *worker_args;
	struct memory_accounting mem_acc;
};

#endif /* TRCACHE_INTERNAL_H */
