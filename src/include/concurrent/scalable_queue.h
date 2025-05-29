#ifndef SCQ_H
#define SCQ_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct scalable_queue scalable_queue;

typedef struct scalable_queue scq;

struct scalable_queue *scq_init(void);

void scq_destroy(struct scalable_queue *scq);

void scq_enqueue(struct scalable_queue *scq, void *datum);

bool scq_dequeue(struct scalable_queue *scq, void **datum);

#endif /* SCQ_H */
