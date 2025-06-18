/**
 * @file   concurrent/scalable_queue.c
 * @brief  Implementation of the scalable_queue data structure.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>

#include "trcache.h"

#include "concurrent/scalable_queue.h"
#include "utils/log.h"
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
 * During initialization, the scalable_queue is assigned a unique ID.
 * This ID is later used when threads access the dequeued nodes.
 */
_Atomic int global_scq_id_flag;
static int global_scq_id_arr[MAX_SCQ_NUM];

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
	int last_dequeued_thread_idx;
};

_Thread_local static struct scq_tls_data *tls_data_ptr_arr[MAX_SCQ_NUM];

/*
 * scalable_queue - Main data structure to manage queue
 * @tls_data_ptr_list: Each thread's scq_tls_data pointer.
 * @spinlock:          Spinlock to manage thread-local data structures.
 * @scq_id:            Global ID of the scalable_queue.
 * @thread_num:        Number of threads.
 */
struct scalable_queue {
	struct scq_tls_data *tls_data_ptr_list[MAX_THREAD_NUM];
	pthread_spinlock_t spinlock;
	int scq_id;
	int thread_num;
};

_Thread_local static struct scalable_queue *tls_scq_ptr_arr[MAX_SCQ_NUM];

/**
 * @brief   Create a new scalable_queue instance.
 *
 * @return  Pointer to queue on success, NULL on failure.
 */
struct scalable_queue *scq_init(void)
{
	struct scalable_queue *scq = calloc(1, sizeof(struct scalable_queue));

	if (scq == NULL) {
		errmsg(stderr, "Queue allocation failed\n");
		return NULL;
	}

	scq->thread_num = 0;

	if (pthread_spin_init(&scq->spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
		errmsg(stderr, "Initialization of spinlock failed\n");
		free(scq);
		return NULL;
	}

	/* Get the spinlock to assign scq id */
	while (atomic_exchange(&global_scq_id_flag, 1) == 1) {
		__asm__ __volatile__("pause");
	}

	scq->scq_id = -1;
	for (int i = 0; i < MAX_SCQ_NUM; i++) {
		if (atomic_load(&global_scq_id_arr[i]) == 0) {
			atomic_store(&global_scq_id_arr[i], 1);
			scq->scq_id = i;
			break;
		}
	}

	atomic_store(&global_scq_id_flag, 0);

	/* Invalid id */
	if (scq->scq_id == -1) {
		errmsg(stderr, "Invalid scq_id\n");
		free(scq);
		return NULL;
	}

	return scq;
}

/**
 * @brief   Destroy the given scalable_queue and free all resources.
 *
 * @param   scq: Queue instance returned by scq_init().
 */
void scq_destroy(struct scalable_queue *scq)
{
	struct scq_tls_data *tls_data_ptr;
	struct scq_dequeued_node_list *dequeued_node_list;
	struct scq_free_node_list *free_node_list;
	struct scq_node *node, *prev_node;

	if (scq == NULL) {
		return;
	}

	assert(scq->scq_id >= 0 && scq->scq_id < MAX_SCQ_NUM);

	/* Get the spinlock to return scq id */
	while (atomic_exchange(&global_scq_id_flag, 1) == 1) {
		__asm__ __volatile__("pause");
	}

	atomic_store(&global_scq_id_arr[scq->scq_id], 0);

	atomic_store(&global_scq_id_flag, 0);

	/* Free thread-local linked lists */
	for (int i = 0; i < scq->thread_num; i++) {
		tls_data_ptr = scq->tls_data_ptr_list[i];
		dequeued_node_list = &tls_data_ptr->dequeued_node_list;
		free_node_list = &tls_data_ptr->free_node_list;

		if (dequeued_node_list->local_initial_head != NULL) {
			node = dequeued_node_list->local_initial_head;
			while (node != dequeued_node_list->local_tail) {
				prev_node = node;
				node = node->next;
				free(prev_node);
				memstat_sub(MEMSTAT_SCQ_NODE,
					sizeof(struct scq_node));
			}
			
			free(dequeued_node_list->local_tail);
			memstat_sub(MEMSTAT_SCQ_NODE,
				sizeof(struct scq_node));
		}

		if (free_node_list->local_head != NULL) {
			node = free_node_list->local_head;
			while (node != free_node_list->local_tail) {
				prev_node = node;
				node = node->next;
				free(prev_node);
				memstat_sub(MEMSTAT_SCQ_NODE,
					sizeof(struct scq_node));
			}
			free(free_node_list->local_tail);
			memstat_sub(MEMSTAT_SCQ_NODE,
				sizeof(struct scq_node));
		}

		node = free_node_list->shared_sentinel.next;
		while (node != NULL) {
			prev_node = node;
			node = node->next;
			free(prev_node);
			memstat_sub(MEMSTAT_SCQ_NODE,
				sizeof(struct scq_node));
		}

		node = tls_data_ptr->shared_sentinel.next;
		while (node != NULL) {
			prev_node = node;
			node = node->next;
			free(prev_node);
			memstat_sub(MEMSTAT_SCQ_NODE,
				sizeof(struct scq_node));
		}

		free(tls_data_ptr);
	}

	pthread_spin_destroy(&scq->spinlock);

	free(scq);
}

/**
 * @brief   Ensure thread-local data exists for the calling thread.
 */
static void check_and_init_scq_tls_data(struct scalable_queue *scq)
{
	struct scq_tls_data *tls_data = NULL;

	if (tls_scq_ptr_arr[scq->scq_id] == scq) {
		return;
	}

	tls_data = (struct scq_tls_data *)calloc(1, sizeof(struct scq_tls_data));
	tls_data_ptr_arr[scq->scq_id] = tls_data;

	tls_data->dequeued_node_list.local_head = NULL;
	tls_data->dequeued_node_list.local_tail = NULL;
	tls_data->dequeued_node_list.local_initial_head = NULL;

	tls_data->free_node_list.local_head = NULL;
	tls_data->free_node_list.local_tail = NULL;

	tls_data->free_node_list.shared_sentinel.next = NULL;
	tls_data->free_node_list.shared_tail
		= &tls_data->free_node_list.shared_sentinel;

	tls_data->shared_sentinel.next = NULL;
	tls_data->shared_tail = &tls_data->shared_sentinel;

	tls_data->last_dequeued_thread_idx = 0;

	pthread_spin_lock(&scq->spinlock);
	scq->tls_data_ptr_list[scq->thread_num] = tls_data;
	scq->thread_num++;
	pthread_spin_unlock(&scq->spinlock);

	tls_scq_ptr_arr[scq->scq_id] = scq;
}

/**
 * If there is a free node available, return it; otherwise allocate one.
 */
static struct scq_node *scq_allocate_node(struct scq_tls_data *tls_data)
{
	struct scq_node *node = NULL;
	struct scq_free_node_list *free_node_list = &tls_data->free_node_list;

	if (free_node_list->local_head == NULL) {
		if (free_node_list->shared_sentinel.next == NULL) {
			struct scq_node *tmp = malloc(sizeof(struct scq_node));
			if (tmp) {
				memstat_add(MEMSTAT_SCQ_NODE,
					sizeof(struct scq_node));
			}
			return tmp;
		}

		free_node_list->local_head
			= atomic_exchange(&free_node_list->shared_sentinel.next, NULL);

		if (free_node_list->local_head == NULL) {
			struct scq_node *tmp = malloc(sizeof(struct scq_node));
			if (tmp) {
				memstat_add(MEMSTAT_SCQ_NODE,
					sizeof(struct scq_node));
			}
			return tmp;
		}

		free_node_list->local_tail
			= atomic_exchange(&free_node_list->shared_tail,
				&free_node_list->shared_sentinel);
	}

	node = free_node_list->local_head;

	if (free_node_list->local_head == free_node_list->local_tail) {
		free_node_list->local_head = NULL;
		free_node_list->local_tail = NULL;
	} else {
		while (node->next == NULL) {
			__asm__ __volatile__("pause");
		}

		free_node_list->local_head = node->next;
	}

	return node;
}

/**
 * @brief   Enqueue the given datum into the queue.
 *
 * @param   scq:   Queue instance.
 * @param   datum: Pointer or scalar to enqueue.
 */
void scq_enqueue(struct scalable_queue *scq, void *datum)
{
	struct scq_tls_data *tls_data = NULL;
	struct scq_node *node = NULL;
	struct scq_node *prev_tail = NULL;

	check_and_init_scq_tls_data(scq);
	tls_data = tls_data_ptr_arr[scq->scq_id];

	node = scq_allocate_node(tls_data);

	node->datum = datum;
	node->next = NULL;
	__sync_synchronize();

	prev_tail = atomic_exchange(&tls_data->shared_tail, node);
	assert(prev_tail != NULL);

	prev_tail->next = node;
}

/**
 * @brief   Return detached nodes back to the enqueue thread's free list.
 */
static void scq_free_nodes(struct scalable_queue *scq,
        struct scq_node *initial_head_node, struct scq_node *tail_node,
        int enqueue_thread_idx)
{
	struct scq_tls_data *tls_data = scq->tls_data_ptr_list[enqueue_thread_idx];
	struct scq_free_node_list *free_node_list = &tls_data->free_node_list;
	struct scq_node *prev_tail = NULL;

	tail_node->next = NULL;
	__sync_synchronize();

	prev_tail = atomic_exchange(&free_node_list->shared_tail, tail_node);
	assert(prev_tail != NULL);

	prev_tail->next = initial_head_node;
}

/**
 * @brief   Dequeue a node from the thread-local list.
 *
 * @return  true if a datum was popped.
 */
static bool pop_from_dequeued_list(struct scalable_queue *scq,
        struct scq_dequeued_node_list *dequeued_node_list,
        void **datum, int enqueue_thread_idx)
{
	struct scq_node *node = NULL;

	if (dequeued_node_list->local_head == NULL) {
		return false;
	}

	node = dequeued_node_list->local_head;
	*datum = node->datum;

	if (node == dequeued_node_list->local_tail) {
		scq_free_nodes(scq, dequeued_node_list->local_initial_head,
			dequeued_node_list->local_tail, enqueue_thread_idx);

		dequeued_node_list->local_head = NULL;
		dequeued_node_list->local_tail = NULL;
		dequeued_node_list->local_initial_head = NULL;
	} else {
		while (node->next == NULL) {
			__asm__ __volatile__("pause");
		}

		dequeued_node_list->local_head = node->next;
	}

	return true;
}

/**
 * @brief   Dequeue a datum from the scalable_queue.
 *
 * @return  true if an element was dequeued.
 */
bool scq_dequeue(struct scalable_queue *scq, void **datum)
{
	struct scq_dequeued_node_list *dequeued_node_list = NULL;
	struct scq_tls_data *tls_data = NULL, *tls_data_enq_thread = NULL;
	int thread_idx = 0;

	check_and_init_scq_tls_data(scq);

	tls_data = tls_data_ptr_arr[scq->scq_id];
	dequeued_node_list = &tls_data->dequeued_node_list;

	if (pop_from_dequeued_list(scq, dequeued_node_list, datum,
			tls_data->last_dequeued_thread_idx)) {
		return true;
	}

	for (int i = 0; i < scq->thread_num; i++ ) {
		thread_idx = (tls_data->last_dequeued_thread_idx + i) % scq->thread_num;
		tls_data_enq_thread = scq->tls_data_ptr_list[thread_idx];

		if (tls_data_enq_thread->shared_sentinel.next == NULL) {
			continue;
		}

		dequeued_node_list->local_head
			= atomic_exchange(&tls_data_enq_thread->shared_sentinel.next, NULL);

		if (dequeued_node_list->local_head == NULL) {
			continue;
		}

		dequeued_node_list->local_tail
			= atomic_exchange(&tls_data_enq_thread->shared_tail,
				&tls_data_enq_thread->shared_sentinel);

		dequeued_node_list->local_initial_head = dequeued_node_list->local_head;

		tls_data->last_dequeued_thread_idx = thread_idx;

		pop_from_dequeued_list(scq, dequeued_node_list, datum, thread_idx);

		return true;
	}

	return false;
}
