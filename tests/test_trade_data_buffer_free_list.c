#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>

#include "pipeline/trade_data_buffer.h"
#include "utils/list_head.h"
#include <stdlib.h>
#include "trcache.h"

#define NUM_ENTRIES 10000000
#define NUM_CONSUMERS 11

struct consumer_arg {
        struct trade_data_buffer *buf;
        trcache_candle_type type;
};

static _Atomic int producer_done = 0;

static void *consumer_thread(void *arg)
{
	struct consumer_arg *carg = (struct consumer_arg *)arg;
	struct trade_data_buffer_cursor *cursor =
		trade_data_buffer_get_cursor(carg->buf, carg->type);
	int consumed = 0;

	while (consumed < NUM_ENTRIES) {
		struct trcache_trade_data *array = NULL;
		int count = 0;

		int has = trade_data_buffer_peek(carg->buf, cursor, &array, &count);
		if (has && count > 0) {
			for (int i = 0; i < count; i++) {
				assert(array[i].trade_id == (uint64_t)(consumed + i));
			}
			consumed += count;
			trade_data_buffer_consume(carg->buf, cursor, count);
		} else {
			if (atomic_load(&producer_done) && consumed >= NUM_ENTRIES)
				break;
			sched_yield();
		}
	}

	assert(consumed == NUM_ENTRIES);
	printf("Consumed: %d\n", consumed);
	return NULL;
}

int main(void)
{
        struct trcache tc = {0};
        struct consumer_arg args[NUM_CONSUMERS];
        pthread_t threads[NUM_CONSUMERS];
        struct list_head free_list;
        INIT_LIST_HEAD(&free_list);

	for (int i = 0; i < NUM_CONSUMERS; i++) {
		trcache_candle_type type = (trcache_candle_type)(1u << i);
		tc.candle_type_flags |= type;
		args[i].buf = NULL; /* will set after buffer init */
		args[i].type = type;
	}

	struct trade_data_buffer *buf = trade_data_buffer_init(&tc);
	assert(buf != NULL);

	for (int i = 0; i < NUM_CONSUMERS; i++) {
		args[i].buf = buf;
	}

	for (int i = 0; i < NUM_CONSUMERS; i++) {
		int ret = pthread_create(&threads[i], NULL, consumer_thread, &args[i]);
		assert(ret == 0);
	}

        for (int i = 0; i < NUM_ENTRIES; i++) {
                struct trcache_trade_data td = {0};
                td.timestamp = i;
                td.trade_id = i;
                td.price = i * 1.0;
                td.volume = i * 2.0;

                if (buf->next_tail_write_idx == NUM_TRADE_CHUNK_CAP - 1 &&
                        list_empty(&free_list)) {
                        trade_data_buffer_reap_free_chunks(buf, &free_list);
                }

                int ret = trade_data_buffer_push(buf, &td, &free_list);
                assert(ret == 0);
        }

	atomic_store(&producer_done, 1);

	for (int i = 0; i < NUM_CONSUMERS; i++) {
		pthread_join(threads[i], NULL);
	}

	assert(buf->produced_count == NUM_ENTRIES);

	printf("Produced: %ld\n", buf->produced_count);

        trade_data_buffer_destroy(buf);

        while (!list_empty(&free_list)) {
                struct list_head *n = list_get_first(&free_list);
                list_del(n);
                struct trade_data_chunk *chunk = __get_trd_chunk_ptr(n);
                free(chunk);
        }

        return 0;
}
