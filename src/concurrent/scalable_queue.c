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

#include "concurrent/scalable_queue.h"
#include "meta/trcache_internal.h"
#include "utils/log.h"
#include "utils/memstat.h"

/*
 * During initialization, the scalable_queue is assigned a unique ID.
 * This ID is later used when threads access the dequeued nodes.
 */
_Atomic int global_scq_id_flag;
static int global_scq_id_arr[MAX_SCQ_NUM];

/*
 * Global pthread key for scq Thread-Local Storage (TLS).
 * This key points to a thread-local "registration list", which is an
 * array of (struct scq_tls_data *) indexed by scq_id. This allows
 * a single thread to be registered with multiple scq instances.
 */
static pthread_key_t g_scq_key;
static pthread_once_t g_scq_key_once = PTHREAD_ONCE_INIT;
static void scq_tls_destructor(void *arg);

/**
 * @brief   One-time initialization for the global pthread key.
 */
static void scq_key_init(void)
{
	if (pthread_key_create(&g_scq_key, scq_tls_destructor) != 0) {
		errmsg(stderr, "Failed to create pthread key for scq\n");
		return;
	}
}

/**
 * @brief   Initialise a scalble_queue that will account memory limit.
 *
 * @param   tc: Pointer to the parent #trcache instance.
 *
 * @return  New queue on success, NULL on failure.
 */
struct scalable_queue *scq_init(struct trcache *tc)
{
	struct scalable_queue *scq = calloc(1, sizeof(struct scalable_queue));

	if (scq == NULL) {
		errmsg(stderr, "Queue allocation failed\n");
		return NULL;
	}

	if (pthread_spin_init(&scq->spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
		errmsg(stderr, "Initialization of spinlock failed\n");
		free(scq);
		return NULL;
	}

	scq->owner_tc = tc;
	scq->thread_num = 0;
	atomic_init(&scq->node_memory_usage.value, 0);
	atomic_init(&scq->object_memory_usage.value, 0);

	/* Initialize the global pthread key exactly once */
	pthread_once(&g_scq_key_once, scq_key_init);

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
		pthread_spin_destroy(&scq->spinlock);
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
	int num_threads;

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

	num_threads = atomic_load(&scq->thread_num);

	/* Free thread-local linked lists */
	for (int i = 0; i < num_threads; i++) {
		tls_data_ptr = scq->tls_data_ptr_list[i];
		dequeued_node_list = &tls_data_ptr->dequeued_node_list;
		free_node_list = &tls_data_ptr->free_node_list;

		if (dequeued_node_list->local_initial_head != NULL) {
			node = dequeued_node_list->local_initial_head;
			while (node != dequeued_node_list->local_tail) {
				prev_node = node;
				node = node->next;
				free(prev_node);
				mem_sub_atomic(&scq->node_memory_usage.value,
					sizeof(struct scq_node));
			}
			free(dequeued_node_list->local_tail);
			mem_sub_atomic(&scq->node_memory_usage.value,
				sizeof(struct scq_node));
		}

		if (free_node_list->local_head != NULL) {
			node = free_node_list->local_head;
			while (node != free_node_list->local_tail) {
				prev_node = node;
				node = node->next;
				free(prev_node);
				mem_sub_atomic(&scq->node_memory_usage.value,
					sizeof(struct scq_node));
			}
			free(free_node_list->local_tail);
			mem_sub_atomic(&scq->node_memory_usage.value,
				sizeof(struct scq_node));
		}

		node = free_node_list->shared_sentinel.next;
		while (node != NULL) {
			prev_node = node;
			node = node->next;
			free(prev_node);
			mem_sub_atomic(&scq->node_memory_usage.value,
				sizeof(struct scq_node));
		}

		node = tls_data_ptr->shared_sentinel.next;
		while (node != NULL) {
			prev_node = node;
			node = node->next;
			free(prev_node);
			mem_sub_atomic(&scq->node_memory_usage.value,
				sizeof(struct scq_node));
		}

		free(tls_data_ptr);
	}

	pthread_spin_destroy(&scq->spinlock);

	free(scq);
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
	struct scq_node *prev_tail = NULL, *node = NULL, *next = NULL;
	bool memory_pressure = atomic_load_explicit(
		&scq->owner_tc->mem_acc.memory_pressure, memory_order_acquire);

	if (memory_pressure) {
		node = initial_head_node;
		while (node != NULL) {
			next = node->next;
			free(node);
			mem_sub_atomic(&scq->node_memory_usage.value,
				sizeof(struct scq_node));
			if (node == tail_node) {
				break;
			}
			node = next;
		}
		return;
	}

	tail_node->next = NULL;
	__sync_synchronize();

	prev_tail = atomic_exchange(&free_node_list->shared_tail, tail_node);
	assert(prev_tail != NULL);

	prev_tail->next = initial_head_node;
}

/**
 * @brief   Cleans up local caches for a tls_data structure.
 *
 * This function moves all nodes from the local free list and local
 * dequeued list (re-publishing data) to their respective shared lists,
 * making them accessible to other threads.
 *
 * @param   tls_data: The tls_data struct to clean up.
 */
static void scq_tls_data_cleanup(struct scq_tls_data *tls_data)
{
	struct scq_free_node_list *free_list = &tls_data->free_node_list;
	struct scq_dequeued_node_list *deq_list = &tls_data->dequeued_node_list;
	struct scq_node *prev_tail;
	struct scq_node *last_used_node = NULL;

	/* 1. Dump local free list to shared free list */
	if (free_list->local_head != NULL) {
		free_list->local_tail->next = NULL;
		__sync_synchronize();

		prev_tail = atomic_exchange(&free_list->shared_tail,
			free_list->local_tail);
		prev_tail->next = free_list->local_head;

		free_list->local_head = NULL;
		free_list->local_tail = NULL;
	}

	/* 2. Process partially consumed dequeued list */
	if (deq_list->local_initial_head != NULL) {
		/*
		 * We must have a valid owner index if we have a list.
		 * This index was set when we stole the batch.
		 */
		assert(tls_data->last_dequeued_thread_idx >= 0);

		/* 
		 * 2a. (Used Nodes) Return [initial_head...head-1] to owner's free list.
		 */
		if (deq_list->local_initial_head != deq_list->local_head) {
			/* Find the node just before local_head */
			last_used_node = deq_list->local_initial_head;
			while (last_used_node != NULL &&
				last_used_node->next != deq_list->local_head) {
				last_used_node = last_used_node->next;
			}

			scq_free_nodes(tls_data->owner_scq, deq_list->local_initial_head,
				last_used_node, tls_data->last_dequeued_thread_idx);
		}

		/* 
		 * 2b. (Unused Nodes) Re-publish [head...tail] to *this* thread's
		 *     enqueue list.
		 */
		deq_list->local_tail->next = NULL;
		__sync_synchronize();

		prev_tail = atomic_exchange(&tls_data->shared_tail,
			deq_list->local_tail);
		prev_tail->next = deq_list->local_head;

		/* 2c. Clear the deq_list state */
		deq_list->local_head = NULL;
		deq_list->local_tail = NULL;
		deq_list->local_initial_head = NULL;
		tls_data->last_dequeued_thread_idx = 0; /* Reset index */
	}
}

/**
 * @brief   Internal unregister logic for "permanent" thread termination.
 *
 * Cleans up local lists and marks the slot as inactive (reusable).
 * This is called by the pthread_key_t destructor.
 *
 * @param   tls_data: The tls_data struct to terminate.
 */
static void scq_thread_terminate_internal(struct scq_tls_data *tls_data)
{
    /*
	 * This function is only called from the destructor, which is the
     * sole owner of the registration list. is_active must be true.
     */
    assert(atomic_load_explicit(&tls_data->is_active,
		memory_order_acquire) == true);

    /* 1. Clean up all local lists first. */
    scq_tls_data_cleanup(tls_data);

    /*
	 * 2. Now that cleanup is done, mark as inactive (terminated).
     *    This makes the slot available for reuse by other threads.
     *    Use release semantics to ensure cleanup is visible before this store.
     */
    atomic_store_explicit(&tls_data->is_active, false,
        memory_order_release);
}

/**
 * @brief   Destructor for pthread_key_t.
 *
 * Called automatically on thread exit. Iterates the thread-local
 * "registration list" and permanently terminates all scq registrations
 * associated with the exiting thread.
 *
 * @param   arg: Pointer to the thread-local "registration list"
 * (struct scq_tls_data **).
 */
static void scq_tls_destructor(void *arg)
{
	struct scq_tls_data **registrations = (struct scq_tls_data **)arg;

	if (registrations == NULL) {
		return;
	}

	for (int i = 0; i < MAX_SCQ_NUM; i++) {
		if (registrations[i] != NULL) {
			scq_thread_terminate_internal(registrations[i]);
			registrations[i] = NULL;
		}
	}

	free(registrations);
}

/**
 * @brief   Gets or initializes the scq_tls_data for the current thread.
 *
 * This function handles all registration scenarios:
 * 1. Fast O(1) re-registration (for paused threads).
 * 2. Lock-free O(N) slot reuse (for new threads recycling old slots).
 * 3. Lock-free O(1) new slot allocation (for new threads).
 *
 * @param   scq: The scalable_queue instance.
 *
 * @return  Pointer to the thread's scq_tls_data, or NULL on failure.
 */
static struct scq_tls_data *scq_get_or_init_tls_data(
	struct scalable_queue *scq)
{
	struct scq_tls_data *ptr_to_use = NULL;
	struct scq_tls_data **registrations;
	struct scq_tls_data *tls_data;
	int num_threads, my_idx;

	/* 1. Get thread-local "registration list" */
	registrations = (struct scq_tls_data **)pthread_getspecific(g_scq_key);
	if (registrations == NULL) {
		registrations = calloc(MAX_SCQ_NUM, sizeof(struct scq_tls_data *));
		if (registrations == NULL) {
			errmsg(stderr, "Failed to alloc scq registration list\n");
			return NULL;
		}
		if (pthread_setspecific(g_scq_key, registrations) != 0) {
			errmsg(stderr, "Failed to set scq registration list\n");
			free(registrations);
			return NULL;
		}
	}

	tls_data = registrations[scq->scq_id];

	/* 2. Case 1: Fast O(1) Path (Already registered) */
	if (tls_data != NULL) {
		if (atomic_load_explicit(&tls_data->is_active,
				memory_order_acquire) == true) {
			/*
			 * This thread is the owner and the slot is active
			 * (either in use or "paused"). This is the hot path.
			 */
			return tls_data;
		} else {
			/*
			 * Error: This thread remembers a slot that was terminated.
			 * This implies scq_tls_destructor ran. This thread's
			 * registration list is stale.
			 */
			errmsg(stderr, "Thread accessing a terminated scq slot\n");
			registrations[scq->scq_id] = NULL; /* Clear stale entry */
			return NULL;
		}
	}

	pthread_spin_lock(&scq->spinlock);

	/* 3. Case 2: Slow Path (New Thread) */
	num_threads = scq->thread_num;
	
	/* 3a. [Lock-Free O(N) Slot Reuse] */
	for (int i = 0; i < num_threads; i++) {
		struct scq_tls_data *ptr = atomic_load_explicit(
			&scq->tls_data_ptr_list[i], memory_order_acquire);

		assert(ptr != NULL);

		if (atomic_load(&ptr->is_active) == false) {
			/* --- Slot Reuse Success --- */
			ptr_to_use = ptr;
			goto slot_acquired;
		}
	}

	/* 3b. [Lock-Free O(1) New Slot Allocation] */
	my_idx = scq->thread_num++;
	if (my_idx >= MAX_THREAD_NUM) {
		scq->thread_num--; /* Rollback */
		errmsg(stderr, "scq_init: MAX_THREAD_NUM reached\n");
		pthread_spin_unlock(&scq->spinlock);
		return NULL;
	}

	ptr_to_use = calloc(1, sizeof(struct scq_tls_data));
	if (ptr_to_use == NULL) {
		errmsg(stderr, "Failed to alloc scq_tls_data\n");
		scq->thread_num--; 
		pthread_spin_unlock(&scq->spinlock);
		/* Note: this leaks a slot in thread_num, but is a fatal error */
		return NULL;
	}

	/*
	 * is_active is set before storing in the list, so other threads
	 * see a fully initialized state.
	 */
	atomic_store_explicit(&ptr_to_use->is_active, true, memory_order_release);
	
	/*
	 * Initialize shared lists (only for new allocation).
	 */
	ptr_to_use->free_node_list.shared_sentinel.next = NULL;
	ptr_to_use->free_node_list.shared_tail =
		&ptr_to_use->free_node_list.shared_sentinel;

	ptr_to_use->shared_sentinel.next = NULL;
	ptr_to_use->shared_tail = &ptr_to_use->shared_sentinel;
	
	atomic_store_explicit(&scq->tls_data_ptr_list[my_idx], ptr_to_use,
		memory_order_release);

slot_acquired:
	/*
	 * Common initialization for reused or new slots.
	 * We must zero out all *local* state.
	 * Shared lists are preserved if reused.
	 */
	ptr_to_use->dequeued_node_list.local_head = NULL;
	ptr_to_use->dequeued_node_list.local_tail = NULL;
	ptr_to_use->dequeued_node_list.local_initial_head = NULL;

	ptr_to_use->free_node_list.local_head = NULL;
	ptr_to_use->free_node_list.local_tail = NULL;
	
	ptr_to_use->last_dequeued_thread_idx = 0;
	ptr_to_use->owner_scq = scq;
	ptr_to_use->scq_id = scq->scq_id;

	registrations[scq->scq_id] = ptr_to_use; /* Register in thread-local list */

	pthread_spin_unlock(&scq->spinlock);

	return ptr_to_use;
}

/**
 * If there is a free node available, return it; otherwise allocate one.
 */
static struct scq_node *scq_allocate_node(struct scalable_queue *scq,
	struct scq_tls_data *tls_data)
{
	struct scq_node *node = NULL;
	struct scq_free_node_list *free_node_list = &tls_data->free_node_list;
	int num_threads, start_idx;

	/* 1. Try local cache */
	if (free_node_list->local_head != NULL) {
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

	/* 2. Try own shared list */
	if (free_node_list->shared_sentinel.next != NULL) {
		free_node_list->local_head
			= atomic_exchange(&free_node_list->shared_sentinel.next, NULL);

		if (free_node_list->local_head != NULL) {
			free_node_list->local_tail
				= atomic_exchange(&free_node_list->shared_tail,
					&free_node_list->shared_sentinel);

			/* Pop one node from the newly acquired local list */
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
	}

	/* 3. Try stealing from other threads' shared lists (is_active ignored) */
	num_threads = atomic_load_explicit(&scq->thread_num,
		memory_order_acquire);
	start_idx = tls_data->last_dequeued_thread_idx;

	for (int i = 0; i < num_threads; i++) {
		int idx = (start_idx + i) % num_threads;
		struct scq_tls_data *other_tls = atomic_load_explicit(
			&scq->tls_data_ptr_list[idx], memory_order_acquire);

		/* Skip self, empty, or unallocated slots */
		if (other_tls == NULL || other_tls == tls_data ||
			other_tls->free_node_list.shared_sentinel.next == NULL) {
			continue;
		}

		/* Try to steal */
		free_node_list->local_head = atomic_exchange(
			&other_tls->free_node_list.shared_sentinel.next, NULL);

		if (free_node_list->local_head != NULL) {
			free_node_list->local_tail = atomic_exchange(
				&other_tls->free_node_list.shared_tail,
				&other_tls->free_node_list.shared_sentinel);

			/* Pop one node from the stolen list */
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
	}

	/* 4. Malloc as last resort */
	node = (struct scq_node *)malloc(sizeof(struct scq_node));
	if (node != NULL) {
		mem_add_atomic(&scq->node_memory_usage.value,
			sizeof(struct scq_node));
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

	tls_data = scq_get_or_init_tls_data(scq);
	if (tls_data == NULL) {
		errmsg(stderr, "Failed to get/init TLS for enqueue\n");
		return; /* Drop data */
	}

	node = scq_allocate_node(scq, tls_data);
	if (node == NULL) {
		errmsg(stderr, "Failed to allocate scq_node for enqueue\n");
		return; /* Drop data */
	}

	node->datum = datum;
	node->next = NULL;
	__sync_synchronize();

	prev_tail = atomic_exchange(&tls_data->shared_tail, node);
	assert(prev_tail != NULL);

	prev_tail->next = node;
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

	tls_data = scq_get_or_init_tls_data(scq);

	if (tls_data == NULL) {
		errmsg(stderr, "Failed to get/init TLS for dequeue\n");
		return false;
	}

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

/**
 * @brief   Explicitly unregister a thread from an scq ("pause").
 *
 * @param   scq: Queue instance to pause.
 */
void scq_thread_unregister(struct scalable_queue *scq)
{
	struct scq_tls_data **registrations;
	struct scq_tls_data *tls_data;

	if (scq == NULL) {
		return;
	}

	registrations = (struct scq_tls_data **)pthread_getspecific(g_scq_key);
	if (registrations == NULL) {
		return; /* Not registered with any scq */
	}

	tls_data = registrations[scq->scq_id];
	if (tls_data == NULL ||
		atomic_load(&tls_data->is_active) == false) {
		return; /* Not registered with this scq, or already terminated */
	}

	/*
	 * Clean up local lists.
	 * is_active remains true, and registration remains.
	 */
	scq_tls_data_cleanup(tls_data);
}
