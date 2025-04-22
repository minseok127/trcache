#ifndef SCQ_H
#define SCQ_H
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct scalable_queue scalable_queue;

typedef struct scalable_queue scq;

struct scalable_queue *scq_init(void);

void scq_destroy(struct scalable_queue *scq);

void scq_enqueue(struct scalable_queue *scq, uint64_t datum);

bool scq_dequeue(struct scalable_queue *scq, uint64_t *datum);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* SCQ_H */
