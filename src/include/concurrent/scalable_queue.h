#ifndef SCQ_H
#define SCQ_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#include "utils/memstat.h"

#define MAX_SCQ_NUM (1024)
#define MAX_THREAD_NUM (1024)

/*
 * scq_node - Linked list node
 * @next:  Pointer to the next inserted node.
 * @datum: Scalar or pointer value.
 *
 * When scq_enqueue is called, an scq_node is allocated and inserted into the
 * linked list queue. When scq_dequeue is called, the nodes are detached from
 * the shared linked list and attached into the thread-local linked list.
 */
struct scq_node {
	struct scq_node *next;
	void *datum;
};

/*
 * Dequeue thread detaches nodes from the shared linked list and brings them
 * into its thread-local linked list.
 */
struct scq_dequeued_node_list {
	struct scq_node *local_head;
	struct scq_node *local_tail;
	struct scq_node *local_initial_head;
};

/*
 * shared_sentinel and shared_tail are used for every dequeue thread.
 * They will push the free node into the shared linked list.
 *
 * local_head and local_tail are used for enqueue thread only. The thread will
 * detach the nodes from the shared linked list into the local list.
 */
struct scq_free_node_list {
	struct scq_node shared_sentinel;
	struct scq_node *shared_tail;
	struct scq_node *local_head;
	struct scq_node *local_tail;
};

/*
 * New nodes are inserted into tail.
 * Thread idx is used to determine the start index of round-robin.
 */
struct scq_tls_data {
	struct scq_dequeued_node_list dequeued_node_list;
	struct scq_free_node_list free_node_list;
	struct scq_node *shared_tail;
	struct scq_node shared_sentinel;
	struct scalable_queue *owner_scq;
	int last_dequeued_thread_idx;
	int scq_id;
	_Atomic bool is_active;
};

/*
 * scalable_queue - Main data structure to manage queue
 * @tls_data_ptr_list: Each thread's scq_tls_data pointer.
 * @scq_id:            Global ID of the scalable_queue.
 * @thread_num:        Number of threads.
 * @mem_acc:           Memory accounting information for this queue.
 */
struct scalable_queue {
	struct scq_tls_data *tls_data_ptr_list[MAX_THREAD_NUM];
	int scq_id;
	_Atomic int thread_num;
	struct trcache *owner_tc;
	struct memory_accounting *mem_acc;
};

typedef struct scalable_queue scalable_queue;

/** Convenience typedef. */
typedef struct scalable_queue scq;

/**
 * @brief   Initialise a scalble_queue that will account memory limit.
 *
 * @param   mem_acc: Pointer to #memory_accounting data (may be NULL).
 *
 * @return  New queue on success, NULL on failure.
 */
struct scalable_queue *scq_init(struct memory_accounting *mem_acc);

/**
 * @brief   Destroy a scalable_queue and free all associated memory.
 */
void scq_destroy(struct scalable_queue *scq);

/**
 * @brief   Enqueue a datum into the queue.
 *
 * @param   scq:   Queue instance.
 * @param   datum: Pointer of scalar to enqueue.
 */
void scq_enqueue(struct scalable_queue *scq, void *datum);

/**
 * @brief   Dequeue a datum from the queue.
 *
 * @param   scq:   Queue instance.
 * @param   datum: Output pointer to store dequeued datum.
 *
 * @return  true if an element was dequeued.
 */
bool scq_dequeue(struct scalable_queue *scq, void **datum);

/**
 * @brief   Explicitly unregister a thread from an scq ("pause").
 *
 * This function signals that the current thread is temporarily pausing its
 * use of the queue. It dumps all local caches (free nodes, pending data)
 * to the shared lists so other threads can access them.
 *
 * The thread retains its "registration" and can re-register (re-activate)
 * instantly (O(1)) on its next call to scq_enqueue/scq_dequeue.
 *
 * @param   scq: Queue instance to pause.
 */
void scq_thread_unregister(struct scalable_queue *scq);

#endif /* SCQ_H */
