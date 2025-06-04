#ifndef SCQ_H
#define SCQ_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct scalable_queue scalable_queue;

/** Convenience typedef. */
typedef struct scalable_queue scq;

/**
 * @brief   Create a new scalable_queue instance.
 *
 * @return  Pointer to queue on success, NULL on failure.
 */
struct scalable_queue *scq_init(void);

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

#endif /* SCQ_H */
