/**
 * @file   admin_thread.c
 * @brief  Implementation of the admin thread main routine.
 */
#define _GNU_SOURCE
#include <math.h>
#include <sched.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <time.h>

#include "concurrent/scalable_queue.h"
#include "sched/admin_thread.h"
#include "meta/symbol_table.h"
#include "meta/trcache_internal.h"
#include "pipeline/trade_data_buffer.h"
#include "pipeline/candle_chunk_list.h"
#include "pipeline/candle_chunk_index.h"
#include "sched/sched_pipeline_stats.h"
#include "sched/worker_thread.h"
#include "utils/log.h"
#include "utils/tsc_clock.h"
#include "utils/memstat.h"

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
 * @brief   Helper to calculate bitmap size in bytes (padded to uint64_t).
 */
static inline size_t get_bitmap_bytes(int num_bits)
{
	int num_words = (num_bits + (BITS_PER_WORD - 1)) / BITS_PER_WORD;
	return num_words * sizeof(uint64_t);
}

/**
 * @brief   Initialise the admin thread state.
 *
 * @param   tc: Owner of the admin state.
 *
 * @return  0 on success, -1 on failure.
 */
int admin_state_init(struct trcache *tc)
{
	struct admin_state *state;
	size_t num_tasks, snaps_size, rates_size, costs_size;
	size_t num_workers, in_mem_words, flush_words;
	size_t im_bitmap_alloc_size, flush_bitmap_alloc_size;

	if (!tc) {
		errmsg(stderr, "Invalid trcache pointer\n");
		return -1;
	}

	state = &(tc->admin_state);
	state->done = false;

	/* 1. Allocate admin-local statistics arrays */
	num_tasks = (size_t)tc->num_candle_configs * (size_t)tc->max_symbols;
	snaps_size = num_tasks * sizeof(struct sched_stage_snapshot);
	rates_size = num_tasks * sizeof(struct sched_stage_rate);
	costs_size = num_tasks * sizeof(struct admin_task_costs);

	state->stage_snaps = calloc(1, snaps_size);
	state->stage_rates = calloc(1, rates_size);
	state->task_costs = calloc(1, costs_size);

	if (state->stage_snaps == NULL || state->stage_rates == NULL ||
		state->task_costs == NULL) {
		errmsg(stderr, "Admin statistics array allocation failed\n");
		goto cleanup_stats;
	}
	
	/* Initialize snapshot completed_seq to UINT64_MAX */
	for (size_t i = 0; i < num_tasks; i++) {
		state->stage_snaps[i].completed_seq = UINT64_MAX;
	}

	/* 2. Allocate admin-local bitmap buffers (for calculation and comparison) */
	num_workers = (size_t)tc->num_workers;
	in_mem_words = get_bitmap_bytes(num_tasks * 2) / 8;
	flush_words = get_bitmap_bytes(num_tasks) / 8;

	state->in_mem_words_per_worker = in_mem_words;
	state->flush_words_per_worker = flush_words;

	im_bitmap_alloc_size = num_workers * in_mem_words * sizeof(uint64_t);
	flush_bitmap_alloc_size = num_workers * flush_words * sizeof(uint64_t);

	state->next_im_bitmaps = aligned_alloc(CACHE_LINE_SIZE,
		ALIGN_UP(im_bitmap_alloc_size, CACHE_LINE_SIZE));
	state->next_flush_bitmaps = aligned_alloc(CACHE_LINE_SIZE,
		ALIGN_UP(flush_bitmap_alloc_size, CACHE_LINE_SIZE));
	state->current_im_bitmaps = aligned_alloc(CACHE_LINE_SIZE,
		ALIGN_UP(im_bitmap_alloc_size, CACHE_LINE_SIZE));
	state->current_flush_bitmaps = aligned_alloc(CACHE_LINE_SIZE,
		ALIGN_UP(flush_bitmap_alloc_size, CACHE_LINE_SIZE));
	
	if (state->next_im_bitmaps == NULL || state->next_flush_bitmaps == NULL ||
		state->current_im_bitmaps == NULL || state->current_flush_bitmaps == NULL)
	{
		errmsg(stderr, "Admin bitmap buffer allocation failed\n");
		goto cleanup_bitmaps;
	}

	memset(state->next_im_bitmaps, 0, im_bitmap_alloc_size);
	memset(state->next_flush_bitmaps, 0, flush_bitmap_alloc_size);
	memset(state->current_im_bitmaps, 0, im_bitmap_alloc_size);
	memset(state->current_flush_bitmaps, 0, flush_bitmap_alloc_size);

	return 0;

cleanup_bitmaps:
	free(state->next_im_bitmaps);
	free(state->next_flush_bitmaps);
	free(state->current_im_bitmaps);
	free(state->current_flush_bitmaps);

cleanup_stats:
	free(state->stage_snaps);
	free(state->stage_rates);
	free(state->task_costs);

	return -1;
}

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised admin_state pointer.
 */
void admin_state_destroy(struct admin_state *state)
{
	if (state == NULL) {
		return;
	}

	/* Free the dynamically allocated statistics arrays */
	free(state->stage_snaps);
	free(state->stage_rates);
	free(state->task_costs);

	/* Free the dynamically allocated bitmap buffers */
	free(state->next_im_bitmaps);
	free(state->next_flush_bitmaps);
	free(state->current_im_bitmaps);
	free(state->current_flush_bitmaps);
}

#define RATE_EMA_SHIFT (2)     /* 2^2 = 4 */
#define RATE_EMA_MULT  (3)     /* 4 - 1 */

/**
 * @brief   Update 64-bit exponential moving average (N=4, alpha=1/4).
 *
 * @param   ema:   Previous EMA value.
 * @param   val:   New sample value.
 *
 * @return  Updated EMA value.
 *
 * EMA(t) = val(t) * (1/4) + (3/4) * EMA(t-1)
 */
static uint64_t ema_update_u64(uint64_t ema, uint64_t val)
{
	/* Handle initial value (0 from calloc) */
	if (ema == 0) {
		return val; /* First sample */
	}
	return (val + RATE_EMA_MULT * ema) >> RATE_EMA_SHIFT;
}

/**
 * @brief   Capture pipeline counters for one candle type.
 *
 * @param   entry:       Symbol entry containing the pipeline.
 * @param   candle_idx:  Candle type identifier.
 * @param   stage:       Output snapshot structure.
 */
static void snapshot_stage(struct symbol_entry *entry, int candle_idx,
	struct sched_stage_snapshot *stage)
{
	struct candle_chunk_list *list
		= entry->candle_chunk_list_ptrs[candle_idx];
	uint64_t mutable_seq;

	if (entry->trd_buf == NULL || list == NULL) {
		/* Symbol might be partially initialized, skip */
		return;
	}

	stage->produced_seq = entry->trd_buf->produced_count;

	mutable_seq = atomic_load_explicit(&list->mutable_seq, 
		memory_order_acquire);

	if (mutable_seq == UINT64_MAX) {
		stage->completed_seq = UINT64_MAX;
	} else if (mutable_seq == 0) {
		stage->completed_seq = 0;
	} else {
		stage->completed_seq = mutable_seq - 1;
	}

	stage->unflushed_batch_count = atomic_load_explicit(
		&list->unflushed_batch_count, memory_order_acquire);
}

/**
 * @brief   Update throughput EMA for a pipeline stage.
 *
 * @param   r:       Rate structure to update.
 * @param   newc:    New snapshot of counters.
 * @param   oldc:    Previous snapshot of counters (from admin_state).
 * @param   dt_ns:   Elapsed time between snapshots in nanoseconds.
 * @param   cache:   Global cache instance for flush threhold info.
 */
static void update_stage_rate(struct sched_stage_rate *r,
	const struct sched_stage_snapshot *newc,
	const struct sched_stage_snapshot *oldc, uint64_t dt_ns,
	struct trcache *cache)
{
	__uint128_t tmp;
	uint64_t diff;
	
	/*
	 * If no time passed, or this is the first sample,
	 * skip the update. The EMA will just keep its previous value.
	 */
	if (dt_ns == 0) {
		return;
	}

	/*
	 * Calculate the rate of trades/sec (input to APPLY).
	 */
	diff = newc->produced_seq - oldc->produced_seq;
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->produced_rate = ema_update_u64(r->produced_rate, (uint64_t)tmp);

	/*
	 * Calculate the rate of completed candles/sec (input to CONVERT).
	 */
	if (oldc->completed_seq == UINT64_MAX) {
		diff = (newc->completed_seq == UINT64_MAX) ?
			0 : newc->completed_seq + 1;
	} else {
		diff = newc->completed_seq - oldc->completed_seq;
	}
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->completed_rate = ema_update_u64(r->completed_rate, (uint64_t)tmp);

	/*
	 * Calculate the rate of batches becoming flushable (input to FLUSH).
	 */
	if (newc->unflushed_batch_count > cache->flush_threshold) {
		diff = newc->unflushed_batch_count;
	} else {
		diff = 0;
	}
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->flushable_batch_rate = ema_update_u64(r->flushable_batch_rate,
		(uint64_t)tmp);
}

/**
 * @brief   Refresh pipeline statistics for all symbols.
 *
 * This function polls all symbol entries and updates the admin-thread-local
 * statistics arrays in 'admin_state'.
 *
 * @param   cache:  Global cache instance.
 */
void update_all_pipeline_stats(struct trcache *cache)
{
	struct symbol_table *table = cache->symbol_table;
	struct admin_state *admin = &cache->admin_state;
	int num_symbols = atomic_load_explicit((int*)&table->num_symbols,
		memory_order_acquire);
	
	uint64_t now_ns = tsc_cycles_to_ns(tsc_cycles());

	for (int type_idx = 0; type_idx < cache->num_candle_configs; type_idx++) {
		for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
			struct symbol_entry *entry = &table->symbol_entries[sym_idx];
			
			/* Calculate index in the 1D admin-local arrays */
			size_t idx = (size_t)type_idx * (size_t)table->capacity +
				(size_t)sym_idx;
			
			struct sched_stage_snapshot *old_snap = &admin->stage_snaps[idx];
			struct sched_stage_rate *rate = &admin->stage_rates[idx];
			struct sched_stage_snapshot new_snap;
			
			/* 1. Get new snapshot */
			snapshot_stage(entry, type_idx, &new_snap);

			/*
			 * 2. Calculate rate
			 * Check if old snapshot is uninitialized (from calloc or init)
			 */
			uint64_t dt_ns = (old_snap->produced_seq == 0 &&
				old_snap->completed_seq == UINT64_MAX) ?
				0 : (now_ns - old_snap->timestamp_ns);

			update_stage_rate(rate, &new_snap, old_snap, dt_ns, cache);

			/* 3. Store new snapshot */
			new_snap.timestamp_ns = now_ns;
			*old_snap = new_snap;
		}
	}
}

/**
 * @brief Tracks the budget assignment state for a single worker group.
 */
struct budget_assign_state {
	double budget_per_worker;
	double current_budget;
	int current_worker_idx;
	int num_workers_in_group;
	int start_worker_idx;
};

/**
 * @brief   Refresh pipeline *cost* statistics for all symbols.
 *
 * This function reads the EMA cycle costs from all candle_chunk_lists
 * and stores a snapshot in 'admin_state.task_costs'.
 *
 * @param   cache:  Global cache instance.
 */
static void update_all_task_costs(struct trcache *cache)
{
	struct symbol_table *table = cache->symbol_table;
	struct admin_state *admin = &cache->admin_state;
	int num_symbols = atomic_load_explicit((int*)&table->num_symbols,
		memory_order_acquire);

	const double MIN_COST = 1.0; /* Assume 1 cycle if EMA is 0 */
	
	for (int type_idx = 0; type_idx < cache->num_candle_configs; type_idx++) {
		for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
			struct candle_chunk_list *list =
				table->symbol_entries[sym_idx].candle_chunk_list_ptrs[type_idx];
			
			size_t idx = (size_t)type_idx * (size_t)table->capacity +
				(size_t)sym_idx;
			
			double cost_apply = (double)atomic_load_explicit(
				&list->ema_cycles_per_apply, memory_order_acquire);
			double cost_convert = (double)atomic_load_explicit(
				&list->ema_cycles_per_convert, memory_order_acquire);
			double cost_flush = (double)atomic_load_explicit(
				&list->ema_cycles_per_flush, memory_order_acquire);

			admin->task_costs[idx].cost_apply =
				(cost_apply > 0) ? cost_apply : MIN_COST;
			admin->task_costs[idx].cost_convert =
				(cost_convert > 0) ? cost_convert : MIN_COST;
			admin->task_costs[idx].cost_flush =
				(cost_flush > 0) ? cost_flush : MIN_COST;
		}
	}
}

/**
 * @brief   Calculates total cycle costs for In-Memory and Flush groups.
 *
 * @param   cache:                 Global cache instance.
 * @param   admin:                 Admin state containing demand stats.
 * @param   symtab:                Symbol table.
 * @param   out_in_memory_cycles:  Total cycles needed for Apply+Convert.
 * @param   out_flush_cycles:      Total cycles needed for Flush.
 */
static void calculate_total_cycle_needs(struct trcache *cache,
	struct admin_state *admin, struct symbol_table *symtab,
	double *out_in_memory_cycles, double *out_flush_cycles)
{
	double total_im_cycles = 0.0;
	double total_flush_cycles = 0.0;
	int num_symbols = atomic_load_explicit((int*)&symtab->num_symbols,
		memory_order_acquire);
	
	for (int type_idx = 0; type_idx < cache->num_candle_configs; type_idx++) {
		for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
			size_t idx = (size_t)type_idx * (size_t)symtab->capacity +
				(size_t)sym_idx;
			
			struct sched_stage_rate *demand = &admin->stage_rates[idx];
			struct admin_task_costs *cost = &admin->task_costs[idx];

			/* Add demand (items per sec) * cost (cycles per item) */
			total_im_cycles
				+= (double)demand->produced_rate * cost->cost_apply;
			total_im_cycles
				+= (double)demand->completed_rate * cost->cost_convert;
			total_flush_cycles
				+= (double)demand->flushable_batch_rate * cost->cost_flush;
		}
	}

	*out_in_memory_cycles = total_im_cycles;
	*out_flush_cycles = total_flush_cycles;
}

/**
 * @brief   Determines worker counts for In-Memory and Flush groups.
 *
 * @param   num_workers:            Total workers available.
 * @param   total_im_cycles:        Total cycles needed for Apply+Convert.
 * @param   total_flush_cycles:     Total cycles needed for Flush.
 * @param   out_num_im_workers:     (out) Number of workers for In-Memory.
 * @param   out_num_flush_workers:  (out) Number of workers for Flush.
 */
static void calculate_worker_group_distribution(int num_workers,
	double total_im_cycles, double total_flush_cycles,
	int *out_num_im_workers, int *out_num_flush_workers)
{
	double total_cycles = total_im_cycles + total_flush_cycles;

	if (total_cycles == 0) {
		*out_num_im_workers = num_workers; /* Default: all to in-memory */
		*out_num_flush_workers = 0;
	} else {
		*out_num_im_workers = (int)ceil(
			(total_im_cycles / total_cycles) * num_workers);
		*out_num_flush_workers = num_workers - *out_num_im_workers;

		/*
		 * Ensure flush always gets at least one worker if it has work,
		 * and In-Memory always gets one if it has work.
		 */
		if (total_flush_cycles > 0 && *out_num_flush_workers == 0 &&
				*out_num_im_workers > 0) {
			*out_num_flush_workers = 1;
			*out_num_im_workers = num_workers - 1;
		} else if (total_im_cycles > 0 && *out_num_im_workers == 0 &&
				*out_num_flush_workers > 0) {
			*out_num_im_workers = 1;
			*out_num_flush_workers = num_workers - 1;
		}
	}
}

/**
 * @brief   Sets a specific bit in a bitmap.
 */
static inline void set_bitmap_bit(uint64_t *bitmap, int bit_index)
{
	bitmap[WORD_OFFSET(bit_index)] |= (1ULL << BIT_OFFSET(bit_index));
}

/**
 * @brief   Helper function to assign a task to a worker in a budget group.
 *
 * It checks the budget, advances the worker if necessary, and sets the
 * corresponding bit in the correct local bitmap.
 *
 * @param   state:             Mutable state of the budget group.
 * @param   task_cost:         The cycle cost of the task to assign.
 * @param   local_bitmaps_all: Pointer to the start of all local bitmaps.
 * @param   words_per_bitmap:  Size of a single worker's bitmap (in words).
 * @param   bit_index:         The bit index to set for this task.
 */
static void assign_task_to_budget_group(struct budget_assign_state *state,
	double task_cost, uint64_t *local_bitmaps_all, size_t words_per_bitmap,
	int bit_index)
{
	if (state->num_workers_in_group == 0) {
		return;
	}

	/*
	 * Check if budget is exceeded AND we can move to a new worker.
	 * (The last worker in the group takes all remaining tasks)
	 */
	int max_worker_idx_in_group = state->start_worker_idx +
		state->num_workers_in_group - 1;

	if (state->current_budget < task_cost &&
		state->current_worker_idx < max_worker_idx_in_group)
	{
		/* Move to next worker */
		state->current_worker_idx++;
		state->current_budget = state->budget_per_worker;
	}

	/* Assign task to the current worker's bitmap */
	uint64_t *worker_bitmap_start = local_bitmaps_all +
		(state->current_worker_idx * words_per_bitmap);
	
	set_bitmap_bit(worker_bitmap_start, bit_index);
	state->current_budget -= task_cost;
}

/**
 * @brief   Assigns all tasks to worker bitmaps based on cycle budgets.
 *
 * This function calculates the *next* assignment and stores it in
 * the admin_state's 'next_...' bitmap buffers.
 *
 * @param   cache:              Global cache instance.
 * @param   admin:              Admin state containing demand stats.
 * @param   symtab:             Symbol table.
 * @param   num_im_workers:     Number of workers in In-Memory group.
 * @param   num_flush_workers:  Number of workers in Flush group.
 * @param   total_im_cycles:    Total cycle budget for the In-Memory group.
 * @param   total_flush_cycles: Total cycle budget for the Flush group.
 */
static void assign_tasks_to_bitmaps(struct trcache *cache,
	struct admin_state *admin,	struct symbol_table *symtab,
	int num_im_workers, int num_flush_workers,
	double total_im_cycles, double total_flush_cycles)
{
	int num_symbols = atomic_load_explicit((int*)&symtab->num_symbols,
		memory_order_acquire);
	int max_syms = symtab->capacity;
	size_t in_mem_words = admin->in_mem_words_per_worker;
	size_t flush_words = admin->flush_words_per_worker;
	
	/* Clear the 'next' buffers for recalculation */
	memset(admin->next_im_bitmaps, 0,
		cache->num_workers * in_mem_words * sizeof(uint64_t));
	memset(admin->next_flush_bitmaps, 0,
		cache->num_workers * flush_words * sizeof(uint64_t));

	/* Initialize state for the In-Memory group */
	struct budget_assign_state im_state = {
		.budget_per_worker = (num_im_workers > 0) ?
			total_im_cycles / num_im_workers : 0,
		.current_worker_idx = 0,
		.num_workers_in_group = num_im_workers,
		.start_worker_idx = 0
	};
	im_state.current_budget = im_state.budget_per_worker;

	/* Initialize state for the Flush group */
	struct budget_assign_state flush_state = {
		.budget_per_worker = (num_flush_workers > 0) ?
			total_flush_cycles / num_flush_workers : 0,
		.current_worker_idx = num_im_workers,
		.num_workers_in_group = num_flush_workers,
		.start_worker_idx = num_im_workers
	};
	flush_state.current_budget = flush_state.budget_per_worker;
	
	/* Main assignment loop: (Type -> Symbol) for cache locality */
	for (int type_idx = 0; type_idx < cache->num_candle_configs; type_idx++) {
		for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
			size_t task_idx = (size_t)type_idx * (size_t)max_syms +
				(size_t)sym_idx;
			struct sched_stage_rate *demand = &admin->stage_rates[task_idx];
			struct admin_task_costs *cost = &admin->task_costs[task_idx];
			
			/* Assign In-Memory (Apply) */
			double apply_task_cost
				= (double)demand->produced_rate * cost->cost_apply;
			assign_task_to_budget_group(&im_state, apply_task_cost,
				admin->next_im_bitmaps, in_mem_words, (task_idx * 2));

			/* Assign In-Memory (Convert) */
			double convert_task_cost
				= (double)demand->completed_rate * cost->cost_convert;
			assign_task_to_budget_group(&im_state, convert_task_cost,
				admin->next_im_bitmaps, in_mem_words, (task_idx * 2) + 1);

			/* Assign Flush */
			double flush_task_cost
				= (double)demand->flushable_batch_rate * cost->cost_flush;
			assign_task_to_budget_group(&flush_state, flush_task_cost,
				admin->next_flush_bitmaps, flush_words, task_idx);
		}
	}
}

/**
 * @brief   Compares 'next' bitmaps with 'current'
 *          and updates workers if changed.
 *
 * @param   cache:          Global cache instance.
 * @param   admin:          Admin state containing all bitmap buffers.
 * @param   num_im_workers: Number of workers assigned to In-Memory group.
 */
static void publish_new_assignments_to_workers(struct trcache *cache,
	struct admin_state *admin, int num_im_workers)
{
	size_t in_mem_bytes = admin->in_mem_words_per_worker * sizeof(uint64_t);
	size_t flush_bytes = admin->flush_words_per_worker * sizeof(uint64_t);

	for (int i = 0; i < cache->num_workers; i++) {
		struct worker_state *state = &cache->worker_state_arr[i];
		int old_group = atomic_load_explicit(&state->group_id,
			memory_order_acquire);
		
		if (i < num_im_workers) {
			/* This is an In-Memory worker */
			int new_group = GROUP_IN_MEMORY;
			uint64_t *next_bitmap = admin->next_im_bitmaps +
				(i * admin->in_mem_words_per_worker);
			uint64_t *current_bitmap = admin->current_im_bitmaps +
				(i * admin->in_mem_words_per_worker);

			/* Check if group or bitmap content has changed */
			if (old_group != new_group ||
				memcmp(next_bitmap, current_bitmap, in_mem_bytes) != 0)
			{
				atomic_store_explicit(&state->group_id, new_group,
					memory_order_release);
				memcpy(state->in_memory_bitmap, next_bitmap, in_mem_bytes);
				memset(state->flush_bitmap, 0, flush_bytes);
				
				/* Save copy of new bitmap */
				memcpy(current_bitmap, next_bitmap, in_mem_bytes);
				memset(admin->current_flush_bitmaps +
					(i * admin->flush_words_per_worker), 0, flush_bytes);
			}

		} else {
			/* This is a Flush worker */
			int new_group = GROUP_FLUSH;
			uint64_t *next_bitmap = admin->next_flush_bitmaps +
				(i * admin->flush_words_per_worker);
			uint64_t *current_bitmap = admin->current_flush_bitmaps +
				(i * admin->flush_words_per_worker);

			/* Check if group or bitmap content has changed */
			if (old_group != new_group ||
				memcmp(next_bitmap, current_bitmap, flush_bytes) != 0)
			{
				atomic_store_explicit(&state->group_id, new_group,
					memory_order_release);
				memcpy(state->flush_bitmap, next_bitmap, flush_bytes);
				memset(state->in_memory_bitmap, 0, in_mem_bytes);

				/* Save copy of new bitmap */
				memcpy(current_bitmap, next_bitmap, flush_bytes);
				memset(admin->current_im_bitmaps +
					(i * admin->in_mem_words_per_worker), 0, in_mem_bytes);
			}
		}
	}
}

/**
 * @brief   Balance work assignments across workers.
 *
 * This is the main scheduler function.
 * 1. Calculates total cycle requirements for (Apply+Convert) vs (Flush).
 * 2. Divides workers into two groups based on the cycle ratio.
 * 3. Calculates a cycle "budget" for each worker in each group.
 * 4. Iterates (Type -> Symbol) and assigns tasks to workers' 'next'
 *    local bitmaps, consuming their budget.
 * 5. Compares 'next' bitmaps to 'current' bitmaps and updates
 *    workers only if a change is detected.
 *
 * @param   cache:  Global cache instance.
 */
static void balance_workers(struct trcache *cache)
{
	struct admin_state *admin = &cache->admin_state;
	struct symbol_table *symtab = cache->symbol_table;
	int num_workers = cache->num_workers;
	double total_im_cycles = 0.0;
	double total_flush_cycles = 0.0;
	int num_im_workers = 0;
	int num_flush_workers = 0;
	
	/* 
	 * 1. Calculate total cycle needs.
	 *    (reads from admin->stage_rates and admin->task_costs)
	 */
	calculate_total_cycle_needs(
		cache, admin, symtab, &total_im_cycles, &total_flush_cycles);
	
	/* 2. Determine worker group sizes */
	calculate_worker_group_distribution(num_workers, total_im_cycles,
		total_flush_cycles, &num_im_workers, &num_flush_workers);

	/*
	 * 3. Assign tasks to the 'next' local bitmaps.
	 *    (This function clears 'next_...' bitmaps internally and
	 *     reads from admin->stage_rates and admin->task_costs)
	 */
	assign_tasks_to_bitmaps(cache, admin, symtab, num_im_workers,
		num_flush_workers, total_im_cycles, total_flush_cycles);

	/*
	 * 4. Publish new assignments to workers,
	 *    only if they differ from 'current' assignments.
	 */
	publish_new_assignments_to_workers(cache, admin, num_im_workers);
}

/**
 * @brief   Aggregates all distributed memory counters and updates
 * the global memory_pressure flag.
 *
 * @param   cache:  Global cache instance.
 */
static void update_global_memory_stats(struct trcache *cache)
{
	size_t total_mem = 0;
	struct symbol_table *table = cache->symbol_table;
	int num_symbols = atomic_load_explicit((int*)&table->num_symbols,
		memory_order_acquire);
	bool pressure;

	/* 1. Sum feed thread free lists */
	for (int i = 0; i < MAX_NUM_THREADS; i++) {
		total_mem += mem_get_atomic(
			&cache->mem_acc.feed_thread_free_list_mem[i].value);
	}

	/* 2. Sum all SCQ pools (nodes + objects) */
	total_mem += mem_get_atomic(
		&cache->head_version_pool->node_memory_usage.value);
	total_mem += mem_get_atomic(
		&cache->head_version_pool->object_memory_usage.value);
	
	for (int i = 0; i < cache->num_candle_configs; i++) {
		total_mem += mem_get_atomic(
			&cache->chunk_pools[i]->node_memory_usage.value);
		total_mem += mem_get_atomic(
			&cache->chunk_pools[i]->object_memory_usage.value);
		total_mem += mem_get_atomic(
			&cache->row_page_pools[i]->node_memory_usage.value);
		total_mem += mem_get_atomic(
			&cache->row_page_pools[i]->object_memory_usage.value);
	}

	/* 3. Sum all active symbol-related components */
	for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
		struct symbol_entry *entry = &table->symbol_entries[sym_idx];

		/* Sum TDB memory (struct + active chunks) */
		if (entry->trd_buf) {
			total_mem += mem_get_atomic(&entry->trd_buf->memory_usage.value);
		}

		/* Sum CKL and CKI memory */
		for (int type_idx = 0; type_idx < cache->num_candle_configs;
				type_idx++) {
			struct candle_chunk_list *list
				= entry->candle_chunk_list_ptrs[type_idx];
			if (list) {
				if (list->chunk_index) {
					total_mem += mem_get_atomic(
						&list->chunk_index->memory_usage.value);
				}
			}
		}
	}

	/* 4. Update global total */
	atomic_store_explicit(&cache->mem_acc.total_usage.value,
		total_mem, memory_order_relaxed);

	/* 5. Update pressure flag (e.g., set at 95% of limit) */
	pressure = (total_mem > (cache->total_memory_limit * 0.95));
	if (pressure != atomic_load(&cache->mem_acc.memory_pressure)) {
		atomic_store_explicit(&cache->mem_acc.memory_pressure,
			pressure, memory_order_release);
	}
}

/**
 * @brief   Entry point for the admin thread.
 *
 * Expects a ::trcache pointer as its argument.
 *
 * @param   arg: Pointer to ::trcache.
 *
 * @return  Always returns NULL.
 */
void *admin_thread_main(void *arg)
{
	struct trcache *cache = (struct trcache *)arg;

	while (!cache->admin_state.done) {
		/* 1. Update demand statistics (reads from hardware) */
		update_all_pipeline_stats(cache);

		/* 2. Update cost statistics (reads from candle_chunk_lists) */
		update_all_task_costs(cache);

		/* 3. Re-balance worker assignments */
		balance_workers(cache);

		/* 4. Update global memory usage and pressure flag */
		update_global_memory_stats(cache);
		
		sched_yield();
	}

	return NULL;
}
