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
 * @local_free_list:       Thread-local chunk free-list used by trd_databufs.
 * @trcache_ptr:           Back-pointer to owner trcache instance.
 * @thread_id:             Assigned index in tls_data_ptr_arr[].
 */
struct trcache_tls_data {
	struct ht_hash_table *local_symbol_id_map;
	struct list_head local_free_list;
	struct trcache *trcache_ptr;
	int thread_id;
};

/*
 * trcache - trcache internal state.
 *
 * @mem_acc:                 All modules use &mem_acc to update memory usage.
 * @admin_state:             State structure for admin thread.
 * @stage_ct_mask:           Candle-type ownership mask per stage/worker.
 * @pthread_trcache_key:     Key for pthread_get/setspecific.
 * @tls_id_mutex:            Protects allocation/release of thread IDs.
 * @tls_id_assigned_flag:    _Atomic flags, which slots are in use.
 * @tls_data_ptr_arr:        Pointers to each thread’s tls_data.
 * @symbol_table:            Abstracted symbol table.
 * @candle_configs:          Array of all candle configurations.
 * @num_candle_configs:      Total number of candle configurations.
 * @num_workers:             Number of worker threads.
 * @batch_candle_count:      Number of candles per column batch.
 * @batch_candle_count_pow2: Equal to log2(@batch_candle_count).
 * @flush_threshold:         How many candle batches to buffer before flush.
 * @flush_threshold_pow2:    Equal to log2(@flush_threshold_batches).
 * @worker_state_arr:        Per-worker state array of length @num_workers.
 * @sched_msg_free_list:     Free list for scheduler message objects.
 * @admin_thread:            Handle for admin thread.
 * @worker_threads:          Array of handles for worker threads.
 * @worker_args:             Per-worker argument array used at start.
 * @max_symbols:             Maximum number of symbols.
*/
struct trcache {
	/*
	 * Group 1: Global Memory Accounting (High-Contention Atomics).
	 * Written by ALL threads. mem_acc contains internally padded counters.
	 */
	____cacheline_aligned
	struct memory_accounting mem_acc;

	/*
	 * Group 2: Admin Thread Exclusive Write Area.
	 * Written frequently by the Admin thread, read by workers.
	 */
	____cacheline_aligned
	struct admin_state admin_state;

	/*
	 * Group 3: Admin Thread Scheduler Mask.
	 * Written frequently by Admin thread during scheduling.
	 */
	____cacheline_aligned	
	uint32_t stage_ct_mask[WORKER_STAT_STAGE_NUM][MAX_NUM_THREADS];

	/*
	 * Group 4: TLS Management (High-Contention Mutex).
	 * Accessed by all threads, but only at thread init/exit.
	 */
	____cacheline_aligned
	pthread_key_t pthread_trcache_key;
	pthread_mutex_t tls_id_mutex;
	_Atomic int tls_id_assigned_flag[MAX_NUM_THREADS];
	struct trcache_tls_data *tls_data_ptr_arr[MAX_NUM_THREADS];

	/*
	 * Group 5: Read-Only / "Cold" Pointers and Configuration.
	 * Set at init() and read by all threads. No false sharing risk.
	 */
	____cacheline_aligned
	struct symbol_table *symbol_table;
	struct trcache_candle_config *candle_configs;
	int num_candle_configs;
	int num_workers;
	int batch_candle_count;
	int batch_candle_count_pow2;
	int flush_threshold;
	int flush_threshold_pow2;
	struct worker_state *worker_state_arr;
	sched_work_msg_free_list *sched_msg_free_list;
	pthread_t admin_thread;
	pthread_t *worker_threads;
	struct worker_thread_args *worker_args;
	int max_symbols;

} ____cacheline_aligned;

#endif /* TRCACHE_INTERNAL_H */
