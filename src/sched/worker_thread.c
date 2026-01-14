/**
 * @file   worker_thread.c
 * @brief  Implementation of the worker thread main routine.
 */

#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>

#include "sched/worker_thread.h"
#include "meta/symbol_table.h"
#include "meta/trcache_internal.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "utils/log.h"
#include "utils/tsc_clock.h"

#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))

/*
 * Bit manipulation helpers for 64-bit bitmaps.
 */
#define BITS_PER_WORD (64)
#define WORD_OFFSET(bit) ((bit) / BITS_PER_WORD)
#define BIT_OFFSET(bit) ((bit) % BITS_PER_WORD)
#define IS_BIT_SET(bitmap, bit) \
	(bitmap[WORD_OFFSET(bit)] & (1ULL << BIT_OFFSET(bit)))

/**
 * @brief A 32-byte (256-bit) chunk of zeros, cacheline-aligned.
 * Used as the target for memcmp to quickly skip empty bitmap chunks.
 */
static const uint64_t g_zero_chunk_32b[4] ____cacheline_aligned = {0, 0, 0, 0};

/**
 * @brief   Calculates the size in bytes for a bitmap.
 *
 * @param   num_bits: Total number of bits required.
 *
 * @return  Size in bytes, aligned to sizeof(uint64_t).
 */
static inline size_t get_bitmap_bytes(int num_bits)
{
	int num_words = (num_bits + (BITS_PER_WORD - 1)) / BITS_PER_WORD;
	return num_words * sizeof(uint64_t);
}

/**
 * @brief   Non-atomically update the EMA cycles for a stage.
 *
 * This function is non-atomic and must only be called by a worker
 * thread that has successfully acquired the ownership lock for
 * the corresponding (list, stage) pair.
 *
 * @param   ema_ptr:   Pointer to the list's EMA variable.
 * @param   cycles:    Total cycles spent on the work.
 * @param   count:     Number of items processed.
 */
static inline void update_ema_cycles(_Atomic uint64_t *ema_ptr,
	uint64_t cycles, uint64_t count)
{
	uint64_t old_ema, new_ema;

	if (count == 0) {
		return;
	}

	new_ema = cycles / count; /* T/N */
	old_ema = *ema_ptr; /* Non-atomic read */

	if (old_ema == 0) {
		/* First sample, just set it */
		*ema_ptr = new_ema;
	} else {
		/* Update EMA (N=4, alpha=1/4) */
		*ema_ptr = (new_ema + (3 * old_ema)) / 4;
	}
}

/**
 * @brief   Consume trade data and update row candles (APPLY stage).
 *
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type index.
 */
static void worker_do_apply(struct symbol_entry *entry, int candle_idx)
{
	struct trade_data_buffer *buf = entry->trd_buf;
	struct trade_data_buffer_cursor *cur;
	struct candle_chunk_list *list;
	struct trcache_trade_data *array = NULL;
	int count = 0;
	uint64_t start_cycles, work_count = 0;

	cur = trade_data_buffer_acquire_cursor(buf, candle_idx);
	if (cur == NULL) {
		/* Cursor is busy (e.g., user thread is applying) */
		return;
	}

	start_cycles = tsc_cycles();
	list = entry->candle_chunk_list_ptrs[candle_idx];

	while (trade_data_buffer_peek(buf, cur, &array, &count) && count > 0) {
		for (int i = 0; i < count; i++) {
			candle_chunk_list_apply_trade(list, &array[i]);
		}

		trade_data_buffer_consume(buf, cur, count);
		work_count += (uint64_t)count;
	}

	if (work_count > 0) {
		/* Update per-list EMA instead of per-worker stats */
		update_ema_cycles(&list->ema_cycles_per_apply,
			tsc_cycles() - start_cycles, work_count);
	}

	trade_data_buffer_release_cursor(cur);
}

/**
 * @brief   Convert row candles to a column batch (CONVERT stage).
 *
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type index.
 */
static void worker_do_convert(struct symbol_entry *entry, int candle_idx)
{
	struct candle_chunk_list *list =
		entry->candle_chunk_list_ptrs[candle_idx];
	uint64_t start_cycles = tsc_cycles();
	int converted_count = candle_chunk_list_convert_to_column_batch(list);

	if (converted_count > 0) {
		/* Update per-list EMA instead of per-worker stats */
		update_ema_cycles(&list->ema_cycles_per_convert,
			tsc_cycles() - start_cycles, (uint64_t)converted_count);
	}
}

/**
 * @brief   Flush converted batches (FLUSH stage).
 *
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type index.
 */
static void worker_do_flush(struct symbol_entry *entry, int candle_idx)
{
	struct candle_chunk_list *list =
		entry->candle_chunk_list_ptrs[candle_idx];
	uint64_t start_cycles = tsc_cycles();
	int flushed_batch_count = candle_chunk_list_flush(list);

	if (flushed_batch_count > 0) {
		/* Update per-list EMA instead of per-worker stats */
		update_ema_cycles(&list->ema_cycles_per_flush,
			tsc_cycles() - start_cycles, (uint64_t)flushed_batch_count);
	}
}

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   tc:        Owner of the admin state.
 * @param   worker_id: Numeric identifier for the worker.
 *
 * @return  0 on success, -1 on failure.
 */
int worker_state_init(struct trcache *tc, int worker_id)
{
	struct worker_state *state;
	int num_tasks;
	size_t in_memory_bitmap_bytes, flush_bitmap_bytes;

	if (!tc) {
		errmsg(stderr, "Invalid trcache pointer\n");
		return -1;
	}

	state = &tc->worker_state_arr[worker_id];
	state->worker_id = worker_id;
	state->done = false;

	/*
	 * Calculate bitmap sizes.
	 * Total tasks = num_candle_types * max_symbols
	 * In-memory bitmap needs 2 bits per task (Apply, Convert).
	 * Flush bitmap needs 1 bit per task (Flush).
	 */
	num_tasks = tc->num_candle_configs * tc->max_symbols;
	in_memory_bitmap_bytes = get_bitmap_bytes(num_tasks * 2);
	flush_bitmap_bytes = get_bitmap_bytes(num_tasks);

	/* Allocate cacheline-aligned bitmaps */
	state->in_memory_bitmap = aligned_alloc(
		CACHE_LINE_SIZE, ALIGN_UP(in_memory_bitmap_bytes, CACHE_LINE_SIZE));
	if (state->in_memory_bitmap == NULL) {
		errmsg(stderr, "in_memory_bitmap allocation failed\n");
		return -1;
	}

	state->flush_bitmap = aligned_alloc(
		CACHE_LINE_SIZE, ALIGN_UP(flush_bitmap_bytes, CACHE_LINE_SIZE));
	if (state->flush_bitmap == NULL) {
		errmsg(stderr, "flush_bitmap allocation failed\n");
		free(state->in_memory_bitmap);
		return -1;
	}

	/* Clear bitmaps */
	memset(state->in_memory_bitmap, 0, in_memory_bitmap_bytes);
	memset(state->flush_bitmap, 0, flush_bitmap_bytes);

	/* Default to in-memory group */
	atomic_init(&state->group_id, GROUP_IN_MEMORY);

	return 0;
}

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised worker_state pointer.
 */
void worker_state_destroy(struct worker_state *state)
{
	if (state == NULL) {
		return;
	}

	/* Free bitmaps */
	free(state->in_memory_bitmap);
	free(state->flush_bitmap);
	state->in_memory_bitmap = NULL;
	state->flush_bitmap = NULL;
}

/**
 * @brief   Finds the next set bit in a 64-bit word.
 *
 * @param   work_chunk: The 64-bit word to scan.
 *
 * @return  The index (0-63) of the first set bit.
 */
static inline int get_next_bit_offset(uint64_t work_chunk)
{
	/* Assumes work_chunk != 0 */
	return __builtin_ctzll(work_chunk);
}

/**
 * @brief   Processes a single 64-bit word from the in-memory bitmap.
 *
 * This is the scalar inner loop, called by both the AVX2 path (for
 * non-zero chunks) and the scalar path.
 *
 * @param   cache:      The main trcache instance.
 * @param   work_word:  The 64-bit word containing work bits.
 * @param   word_idx:   The index of this word in the overall bitmap.
 *
 * @return  true if any work was successfully claimed and executed,
 *          false otherwise.
 */
static inline bool process_in_memory_word(struct trcache *cache,
	uint64_t work_word, int word_idx)
{
	struct symbol_table *symtab = cache->symbol_table;
	const int max_syms = symtab->capacity;
	const int num_types = cache->num_candle_configs;
	const int num_tasks = num_types * max_syms;

	const int taken_value = 1;
	bool work_done = false;

	while (work_word != 0) {
		int expected_free = -1; /* Local var for CAS comparison */
		int bit_offset = get_next_bit_offset(work_word);
		int global_bit_idx = (word_idx * BITS_PER_WORD) + bit_offset;
		int task_idx = global_bit_idx / 2;
		bool is_apply = (global_bit_idx % 2) == 0;

		if (task_idx < num_tasks) {
			int type_idx = task_idx / max_syms;
			int sym_idx = task_idx % max_syms;
			_Atomic int *flag;
			struct symbol_entry *entry;

			if (is_apply) {
				flag = &symtab->in_memory_ownership_flags[task_idx]
					.apply_taken;
			} else {
				flag = &symtab->in_memory_ownership_flags[task_idx]
					.convert_taken;
			}

			/*
			 * First, perform a cheap atomic read.
			 * Only attempt the expensive CAS if the flag appears to be free.
			 */
			if (atomic_load_explicit(flag, memory_order_acquire)
					== expected_free) {
				/*
				 * Now, attempt to acquire ownership
				 */
				if (atomic_compare_exchange_strong(
						flag, &expected_free, taken_value)) {
					entry = &symtab->symbol_entries[sym_idx];

					if (is_apply) {
						worker_do_apply(entry, type_idx);
					} else {
						worker_do_convert(entry, type_idx);
					}
					
					atomic_store_explicit(
						flag, -1, memory_order_release);
					
					work_done = true;
				}
				/*
				 * If CAS failed, another worker raced us and won.
				 * We just continue to the next bit.
				 */
			}
		}
		/* Clear the processed bit from the local copy */
		work_word &= ~(1ULL << bit_offset);
	}
	return work_done;
}

/**
 * @brief   Processes a single 64-bit word from the flush bitmap.
 *
 * @param   cache:      The main trcache instance.
 * @param   work_word:  The 64-bit word containing work bits.
 * @param   word_idx:   The index of this word in the overall bitmap.
 *
 * @return  true if any work was successfully claimed and executed,
 *          false otherwise.
 */
static inline bool process_flush_word(struct trcache *cache,
	uint64_t work_word, int word_idx)
{
	struct symbol_table *symtab = cache->symbol_table;
	const int max_syms = symtab->capacity;
	const int num_types = cache->num_candle_configs;
	const int num_tasks = num_types * max_syms;

	const int taken_value = 1;
	bool work_done = false;

	while (work_word != 0) {
		int expected_free = -1; /* Local var for CAS comparison */
		int bit_offset = get_next_bit_offset(work_word);
		int task_idx = (word_idx * BITS_PER_WORD) + bit_offset;

		if (task_idx < num_tasks) {
			_Atomic int *flag = &symtab->flush_ownership_flags[task_idx];

			/*
			 * First, perform a cheap atomic read.
			 * Only attempt the expensive CAS if the flag appears to be free.
			 */
			if (atomic_load_explicit(flag, memory_order_acquire)
					== expected_free) {
				/*
				 * Now, attempt to acquire ownership
				 */
				if (atomic_compare_exchange_strong(
						flag, &expected_free, taken_value)) {
					int type_idx = task_idx / max_syms;
					int sym_idx = task_idx % max_syms;
					struct symbol_entry *entry =
						&symtab->symbol_entries[sym_idx];

					worker_do_flush(entry, type_idx);

					atomic_store_explicit(
						flag, -1, memory_order_release);

					work_done = true;
				}
			}
		}
		/* Clear the processed bit */
		work_word &= ~(1ULL << bit_offset);
	}
	return work_done;
}

/**
 * @brief   Scans the in-memory bitmap and executes Apply/Convert tasks.
 *
 * @param   cache: The main trcache instance.
 * @param   state: The worker's state.
 *
 * @return  true if any work was successfully claimed and executed,
 *          false otherwise.
 */
static bool worker_run_in_memory_tasks(struct trcache *cache,
	struct worker_state *state)
{
	const int num_types = cache->num_candle_configs;
	const int max_syms = cache->symbol_table->capacity;
	const int num_bits = (num_types * max_syms) * 2;
	const int total_words = (get_bitmap_bytes(num_bits) / 8);

	bool work_done = false;
	uint64_t *bitmap = state->in_memory_bitmap;

	const int words_per_chunk = 4; /* 4 * uint64_t = 32 bytes */
	const int num_chunks = total_words / words_per_chunk;

	/* Main loop to skip empty 32-byte chunks */
	for (int i = 0; i < num_chunks; i++) {
		int base_word_idx = i * words_per_chunk;
		uint64_t *chunk_ptr = &bitmap[base_word_idx];

		/*
		 * Check if the 32-byte chunk is ALL zeros using memcmp.
		 */
		if (memcmp(chunk_ptr, g_zero_chunk_32b, 32) != 0) {
			/* This chunk has work. Process the 4 words scalar-style. */
			for (int j = 0; j < words_per_chunk; j++) {
				int word_idx = base_word_idx + j;
				uint64_t work_word = bitmap[word_idx];
				if (work_word != 0) {
					work_done |= process_in_memory_word(
						cache, work_word, word_idx);
				}
			}
		}
	}

	/* Handle any remaining words that don't fit in a full chunk */
	for (int i = num_chunks * words_per_chunk; i < total_words; i++) {
		uint64_t work_word = bitmap[i];
		if (work_word != 0) {
			work_done |= process_in_memory_word(cache, work_word, i);
		}
	}

	return work_done;
}

/**
 * @brief   Scans the flush bitmap and executes Flush tasks.
 *
 * @param   cache: The main trcache instance.
 * @param   state: The worker's state.
 *
 * @return  true if any work was successfully claimed and executed,
 *          false otherwise.
 */
static bool worker_run_flush_tasks(struct trcache *cache,
	struct worker_state *state)
{
	const int num_types = cache->num_candle_configs;
	const int max_syms = cache->symbol_table->capacity;
	const int num_tasks = num_types * max_syms;
	const int total_words = (get_bitmap_bytes(num_tasks) / 8);

	bool work_done = false;
	uint64_t *bitmap = state->flush_bitmap;

	const int words_per_chunk = 4; /* 4 * uint64_t = 32 bytes */
	const int num_chunks = total_words / words_per_chunk;

	/* Main loop to skip empty 32-byte chunks */
	for (int i = 0; i < num_chunks; i++) {
		int base_word_idx = i * words_per_chunk;
		uint64_t *chunk_ptr = &bitmap[base_word_idx];

		/*
		 * Check if the 32-byte chunk is ALL zeros using memcmp.
		 */
		if (memcmp(chunk_ptr, g_zero_chunk_32b, 32) != 0) {
			/* This chunk has work. Process the 4 words scalar-style. */
			for (int j = 0; j < words_per_chunk; j++) {
				int word_idx = base_word_idx + j;
				uint64_t work_word = bitmap[word_idx];
				if (work_word != 0) {
					work_done |= process_flush_word(
						cache, work_word, word_idx);
				}
			}
		}
	}

	/* Handle any remaining words that don't fit in a full chunk */
	for (int i = num_chunks * words_per_chunk; i < total_words; i++) {
		uint64_t work_word = bitmap[i];
		if (work_word != 0) {
			work_done |= process_flush_word(cache, work_word, i);
		}
	}

	return work_done;
}

/**
 * @brief   Entry point for a worker thread.
 *
 * Accepts a pointer to ::worker_thread_args.
 *
 * @param   arg: See ::worker_thread_args.
 *
 * @return  Always returns NULL.
 */
void *worker_thread_main(void *arg)
{
	struct worker_thread_args *args = (struct worker_thread_args *)arg;
	struct trcache *cache = args->cache;
	int worker_id = args->worker_id;
	struct worker_state *state = &cache->worker_state_arr[worker_id];
	bool work_done;
	int old_group = -1;

	while (!state->done) {
		int group = atomic_load_explicit(&state->group_id,
			memory_order_acquire);
		work_done = false;

		/*
		 * If group ID changed, unregister from all SCQ pools
		 * to flush thread-local SCQ node caches.
		 */
		if (old_group != -1 && old_group != group) {
			scq_thread_unregister(cache->head_version_pool);
			for (int i = 0; i < cache->num_candle_configs; i++) {
				scq_thread_unregister(cache->chunk_pools[i]);
				scq_thread_unregister(cache->row_page_pools[i]);
			}
		}
		old_group = group;

		if (group == GROUP_IN_MEMORY) {
			work_done = worker_run_in_memory_tasks(cache, state);
		} else if (group == GROUP_FLUSH) {
			work_done = worker_run_flush_tasks(cache, state);
		}

		/* Busy-wait (Low Latency) */
		if (!work_done) {
			__asm__ __volatile__("pause");
		} 
	}

	/*
	 * Thread is terminating. Unregister from all SCQ pools
	 * one last time to flush any remaining cached nodes.
	 */
	scq_thread_unregister(cache->head_version_pool);
	for (int i = 0; i < cache->num_candle_configs; i++) {
		scq_thread_unregister(cache->chunk_pools[i]);
		scq_thread_unregister(cache->row_page_pools[i]);
	}

	return NULL;
}
