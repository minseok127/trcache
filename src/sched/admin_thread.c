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
#include "pipeline/event_data_buffer.h"
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
	size_t num_workers, in_mem_words, batch_flush_words, trade_flush_words;
	size_t im_bitmap_alloc_size, batch_flush_bitmap_alloc_size;
	size_t trade_flush_bitmap_alloc_size;

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
	state->trade_flush_costs =
		calloc((size_t)tc->max_symbols, sizeof(double));
	state->trade_block_fill_rates =
		calloc((size_t)tc->max_symbols, sizeof(uint64_t));
	state->trade_fill_timestamps =
		calloc((size_t)tc->max_symbols, sizeof(uint64_t));
	state->book_update_costs =
		calloc((size_t)tc->max_symbols, sizeof(double));
	state->book_update_rates =
		calloc((size_t)tc->max_symbols,
			sizeof(uint64_t));
	state->book_produced_snaps =
		calloc((size_t)tc->max_symbols,
			sizeof(uint64_t));
	state->book_block_fill_rates =
		calloc((size_t)tc->max_symbols,
			sizeof(uint64_t));
	state->book_fill_timestamps =
		calloc((size_t)tc->max_symbols,
			sizeof(uint64_t));
	state->book_event_flush_costs =
		calloc((size_t)tc->max_symbols, sizeof(double));

	if (state->stage_snaps == NULL ||
		state->stage_rates == NULL ||
		state->task_costs == NULL ||
		state->trade_flush_costs == NULL ||
		state->trade_block_fill_rates == NULL ||
		state->trade_fill_timestamps == NULL ||
		state->book_update_costs == NULL ||
		state->book_update_rates == NULL ||
		state->book_produced_snaps == NULL ||
		state->book_block_fill_rates == NULL ||
		state->book_fill_timestamps == NULL ||
		state->book_event_flush_costs == NULL) {
		errmsg(stderr,
			"Admin statistics array alloc failed\n");
		goto cleanup_stats;
	}
	
	/* Initialize snapshot completed_seq to UINT64_MAX */
	for (size_t i = 0; i < num_tasks; i++) {
		state->stage_snaps[i].completed_seq = UINT64_MAX;
	}

	/* 2. Allocate admin-local bitmap buffers (for calculation and comparison) */
	num_workers = (size_t)tc->num_workers;
	in_mem_words = get_bitmap_bytes(num_tasks * 2) / 8;
	batch_flush_words = get_bitmap_bytes(num_tasks) / 8;
	/* One bit per symbol (not per candle-type × symbol). */
	trade_flush_words = get_bitmap_bytes(tc->max_symbols) / 8;

	state->in_mem_words_per_worker = in_mem_words;
	state->batch_flush_words_per_worker = batch_flush_words;
	state->trade_flush_words_per_worker = trade_flush_words;
	/*
	 * Book bitmaps have the same layout as trade flush:
	 * 1 bit per symbol, independent of candle type.
	 */
	state->book_update_words_per_worker = trade_flush_words;
	state->book_event_flush_words_per_worker = trade_flush_words;

	im_bitmap_alloc_size =
		num_workers * in_mem_words * sizeof(uint64_t);
	batch_flush_bitmap_alloc_size =
		num_workers * batch_flush_words * sizeof(uint64_t);
	trade_flush_bitmap_alloc_size =
		num_workers * trade_flush_words * sizeof(uint64_t);

	size_t book_bitmap_alloc_size =
		num_workers * trade_flush_words * sizeof(uint64_t);

	state->next_im_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(im_bitmap_alloc_size, CACHE_LINE_SIZE));
	state->next_batch_flush_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(batch_flush_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->next_trade_flush_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(trade_flush_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->current_im_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(im_bitmap_alloc_size, CACHE_LINE_SIZE));
	state->current_batch_flush_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(batch_flush_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->current_trade_flush_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(trade_flush_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->next_book_update_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(book_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->next_book_event_flush_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(book_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->current_book_update_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(book_bitmap_alloc_size,
			CACHE_LINE_SIZE));
	state->current_book_event_flush_bitmaps = aligned_alloc(
		CACHE_LINE_SIZE,
		ALIGN_UP(book_bitmap_alloc_size,
			CACHE_LINE_SIZE));

	if (state->next_im_bitmaps == NULL ||
		state->next_batch_flush_bitmaps == NULL ||
		state->next_trade_flush_bitmaps == NULL ||
		state->current_im_bitmaps == NULL ||
		state->current_batch_flush_bitmaps == NULL ||
		state->current_trade_flush_bitmaps == NULL ||
		state->next_book_update_bitmaps == NULL ||
		state->next_book_event_flush_bitmaps == NULL ||
		state->current_book_update_bitmaps == NULL ||
		state->current_book_event_flush_bitmaps == NULL)
	{
		errmsg(stderr,
			"Admin bitmap buffer alloc failed\n");
		goto cleanup_bitmaps;
	}

	memset(state->next_im_bitmaps, 0,
		im_bitmap_alloc_size);
	memset(state->next_batch_flush_bitmaps, 0,
		batch_flush_bitmap_alloc_size);
	memset(state->next_trade_flush_bitmaps, 0,
		trade_flush_bitmap_alloc_size);
	memset(state->current_im_bitmaps, 0,
		im_bitmap_alloc_size);
	memset(state->current_batch_flush_bitmaps, 0,
		batch_flush_bitmap_alloc_size);
	memset(state->current_trade_flush_bitmaps, 0,
		trade_flush_bitmap_alloc_size);
	memset(state->next_book_update_bitmaps, 0,
		book_bitmap_alloc_size);
	memset(state->next_book_event_flush_bitmaps, 0,
		book_bitmap_alloc_size);
	memset(state->current_book_update_bitmaps, 0,
		book_bitmap_alloc_size);
	memset(state->current_book_event_flush_bitmaps, 0,
		book_bitmap_alloc_size);

	return 0;

cleanup_bitmaps:
	free(state->next_im_bitmaps);
	free(state->next_batch_flush_bitmaps);
	free(state->next_trade_flush_bitmaps);
	free(state->current_im_bitmaps);
	free(state->current_batch_flush_bitmaps);
	free(state->current_trade_flush_bitmaps);
	free(state->next_book_update_bitmaps);
	free(state->next_book_event_flush_bitmaps);
	free(state->current_book_update_bitmaps);
	free(state->current_book_event_flush_bitmaps);

cleanup_stats:
	free(state->stage_snaps);
	free(state->stage_rates);
	free(state->task_costs);
	free(state->trade_flush_costs);
	free(state->trade_block_fill_rates);
	free(state->trade_fill_timestamps);
	free(state->book_update_costs);
	free(state->book_update_rates);
	free(state->book_produced_snaps);
	free(state->book_block_fill_rates);
	free(state->book_fill_timestamps);
	free(state->book_event_flush_costs);

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
	free(state->trade_flush_costs);
	free(state->trade_block_fill_rates);
	free(state->trade_fill_timestamps);
	free(state->book_update_costs);
	free(state->book_update_rates);
	free(state->book_produced_snaps);
	free(state->book_block_fill_rates);
	free(state->book_fill_timestamps);
	free(state->book_event_flush_costs);

	/* Free the dynamically allocated bitmap buffers */
	free(state->next_im_bitmaps);
	free(state->next_batch_flush_bitmaps);
	free(state->next_trade_flush_bitmaps);
	free(state->current_im_bitmaps);
	free(state->current_batch_flush_bitmaps);
	free(state->current_trade_flush_bitmaps);
	free(state->next_book_update_bitmaps);
	free(state->next_book_event_flush_bitmaps);
	free(state->current_book_update_bitmaps);
	free(state->current_book_event_flush_bitmaps);
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

	/*
	 * produced_seq: cumulative number of trades pushed by the feed thread.
	 * Used as the APPLY demand signal — the diff between two snapshots
	 * gives how many trades arrived and need processing.
	 */
	stage->produced_seq = entry->trd_buf->produced_count;

	/*
	 * mutable_seq: the *next* candle slot to be written.
	 * The last *completed* candle is therefore (mutable_seq - 1).
	 * UINT64_MAX is a sentinel meaning no candle has been completed yet
	 * (initial state before the first candle closes).
	 */
	mutable_seq = atomic_load_explicit(&list->mutable_seq,
		memory_order_acquire);

	if (mutable_seq == UINT64_MAX) {
		/* Sentinel: no completed candle yet */
		stage->completed_seq = UINT64_MAX;
	} else if (mutable_seq == 0) {
		stage->completed_seq = 0;
	} else {
		stage->completed_seq = mutable_seq - 1;
	}

	/*
	 * unflushed_batch_count: batches that have been CONVERT-completed
	 * but not yet delivered to the user flush callback.
	 * Used as the FLUSH demand signal.
	 */
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
	/*
	 * __uint128_t is used to avoid overflow when multiplying by 1e9.
	 * For high-frequency symbols, diff can be large enough that
	 * diff * 1,000,000,000 exceeds 2^64, causing silent truncation.
	 */
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
	 * produced_rate: APPLY demand signal (trades/sec).
	 * Measures how fast the feed thread is producing trades that need
	 * to be applied to the in-memory candle pipeline.
	 */
	diff = newc->produced_seq - oldc->produced_seq;
	tmp = (__uint128_t)diff * 1000000000ull;
	tmp /= dt_ns;
	r->produced_rate = ema_update_u64(r->produced_rate, (uint64_t)tmp);

	/*
	 * completed_rate: CONVERT demand signal (candles/sec).
	 * When old completed_seq is UINT64_MAX (initial sentinel), treat
	 * it as -1 so the first real value gives the correct delta
	 * (newc->completed_seq + 1 candles completed in this interval).
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
	 * flushable_batch_rate: FLUSH demand signal.
	 * When the unflushed count is at or below the threshold, the rate
	 * is forced to 0 — there is no urgency to flush yet, and assigning
	 * workers would waste bandwidth on sub-threshold work.
	 */
	if (newc->unflushed_batch_count > cache->batch_flush_threshold) {
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
 * @brief   Update per-symbol trade block fill rates.
 *
 * Tracks how many fully-filled unflushed trade blocks each symbol
 * accumulates per second. This is intentionally kept in dedicated arrays
 * (separate from stage_snaps/stage_rates) because trade flush is
 * per-symbol only and has no candle-type dimension — mixing the two
 * would conflate orthogonal indexing schemes.
 *
 * Mirrors the flushable_batch_rate method: unflushed count / elapsed
 * time gives blocks/sec, fed into an EMA for smoothing.
 *
 * @param   cache:       Global cache instance.
 * @param   admin:       Admin state containing fill-rate arrays.
 * @param   num_symbols: Current number of registered symbols.
 * @param   now_ns:      Current timestamp in nanoseconds.
 */
static void update_trade_block_fill_rates(struct trcache *cache,
	struct admin_state *admin, int num_symbols, uint64_t now_ns)
{
	struct symbol_table *table = cache->symbol_table;

	for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
		struct event_data_buffer *tdb =
			table->symbol_entries[sym_idx].trd_buf;
		uint64_t last_ts = admin->trade_fill_timestamps[sym_idx];
		uint64_t dt_ns = (last_ts == 0) ? 0 : (now_ns - last_ts);
		__uint128_t tmp;
		int unflushed;

		admin->trade_fill_timestamps[sym_idx] = now_ns;

		if (dt_ns == 0 || tdb == NULL) {
			continue;
		}

		unflushed = atomic_load_explicit(
			&tdb->num_full_unflushed_blocks,
			memory_order_acquire);
		tmp = (__uint128_t)(unflushed > 0 ? (uint64_t)unflushed : 0)
			* 1000000000ull;
		tmp /= dt_ns;
		admin->trade_block_fill_rates[sym_idx] =
			ema_update_u64(admin->trade_block_fill_rates[sym_idx],
				(uint64_t)tmp);
	}
}

/**
 * @brief   Update per-symbol book pipeline rates.
 *
 * Computes two independent demand signals:
 *   - book_update_rates:     events/sec from produced_count
 *     diff. Drives the book-update task (GROUP_IN_MEMORY).
 *   - book_block_fill_rates: unflushed blocks/sec, same
 *     method as trade flush. Drives the book-event-flush
 *     task (GROUP_FLUSH).
 *
 * @param   cache:       Global cache instance.
 * @param   admin:       Admin state containing rate arrays.
 * @param   num_symbols: Current number of registered symbols.
 * @param   now_ns:      Current timestamp in nanoseconds.
 */
static void update_book_pipeline_rates(
	struct trcache *cache,
	struct admin_state *admin, int num_symbols,
	uint64_t now_ns)
{
	struct symbol_table *table = cache->symbol_table;

	for (int sym_idx = 0; sym_idx < num_symbols;
			sym_idx++) {
		struct event_data_buffer *buf =
			table->symbol_entries[sym_idx].book_buf;
		uint64_t last_ts =
			admin->book_fill_timestamps[sym_idx];
		uint64_t dt_ns =
			(last_ts == 0) ? 0 : (now_ns - last_ts);
		__uint128_t tmp;

		admin->book_fill_timestamps[sym_idx] = now_ns;

		if (dt_ns == 0 || buf == NULL) {
			continue;
		}

		/*
		 * Update rate: events/sec from produced_count.
		 * Analogous to candle produced_rate.
		 */
		uint64_t cur_produced = buf->produced_count;
		uint64_t old_produced =
			admin->book_produced_snaps[sym_idx];
		admin->book_produced_snaps[sym_idx] =
			cur_produced;

		uint64_t diff = cur_produced - old_produced;
		tmp = (__uint128_t)diff * 1000000000ull;
		tmp /= dt_ns;
		admin->book_update_rates[sym_idx] =
			ema_update_u64(
				admin->book_update_rates[sym_idx],
				(uint64_t)tmp);

		/*
		 * Flush rate: unflushed blocks/sec.
		 * Analogous to trade_block_fill_rates.
		 */
		int unflushed = atomic_load_explicit(
			&buf->num_full_unflushed_blocks,
			memory_order_acquire);
		tmp = (__uint128_t)(unflushed > 0
			? (uint64_t)unflushed : 0)
			* 1000000000ull;
		tmp /= dt_ns;
		admin->book_block_fill_rates[sym_idx] =
			ema_update_u64(
				admin->book_block_fill_rates
					[sym_idx],
				(uint64_t)tmp);
	}
}

/**
 * @brief   Refresh pipeline statistics for all symbols.
 *
 * Updates the admin-thread-local statistics arrays in 'admin_state':
 *   1. Candle pipeline snapshots and throughput rates (type × symbol).
 *   2. Trade block fill rates (symbol only, when trade flush is enabled).
 *   3. Book block fill rates (symbol only, when book is enabled).
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
			 * 2. Calculate rate.
			 * dt_ns = 0 on the first sample (old_snap is zeroed by calloc
			 * except completed_seq which is UINT64_MAX from init).
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

	if (cache->trade_flush_ops.flush != NULL) {
		update_trade_block_fill_rates(
			cache, admin, num_symbols, now_ns);
	}

	if (cache->book_enabled) {
		update_book_pipeline_rates(
			cache, admin, num_symbols, now_ns);
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
			double cost_batch_flush = (double)atomic_load_explicit(
				&list->ema_cycles_per_batch_flush, memory_order_acquire);

			admin->task_costs[idx].cost_apply =
				(cost_apply > 0) ? cost_apply : MIN_COST;
			admin->task_costs[idx].cost_convert =
				(cost_convert > 0) ? cost_convert : MIN_COST;
			admin->task_costs[idx].cost_batch_flush =
				(cost_batch_flush > 0) ? cost_batch_flush : MIN_COST;
		}
	}

	/*
	 * Trade flush cost is per-symbol only, stored in the
	 * dedicated trade_flush_costs[] array.
	 */
	if (cache->trade_flush_ops.flush != NULL) {
		for (int sym_idx = 0; sym_idx < num_symbols;
				sym_idx++) {
			struct event_data_buffer *tdb =
				table->symbol_entries[sym_idx].trd_buf;
			if (tdb == NULL) {
				continue;
			}
			double cost_tf = (double)atomic_load_explicit(
				&tdb->ema_cycles_per_flush,
				memory_order_acquire);

			admin->trade_flush_costs[sym_idx] =
				(cost_tf > 0) ? cost_tf : MIN_COST;
		}
	}

	/*
	 * Book update + book event flush costs are per-symbol
	 * only. ema_cycles_per_update tracks the cost of
	 * calling book_state_ops.update() per event batch.
	 * ema_cycles_per_flush tracks the IO flush cost.
	 */
	if (cache->book_enabled) {
		for (int sym_idx = 0; sym_idx < num_symbols;
				sym_idx++) {
			struct event_data_buffer *buf =
				table->symbol_entries[sym_idx]
					.book_buf;
			if (buf == NULL) {
				continue;
			}
			double cost_upd =
				(double)atomic_load_explicit(
					&buf->ema_cycles_per_update,
					memory_order_acquire);
			double cost_flush =
				(double)atomic_load_explicit(
					&buf->ema_cycles_per_flush,
					memory_order_acquire);

			admin->book_update_costs[sym_idx] =
				(cost_upd > 0)
					? cost_upd : MIN_COST;
			admin->book_event_flush_costs[sym_idx] =
				(cost_flush > 0)
					? cost_flush : MIN_COST;
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
 * @param   out_flush_cycles:      Total cycles needed for Flush group.
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

			/*
			 * rate (items/sec) × cost (cycles/item) = cycles/sec demand.
			 * Summing across all symbols gives the total CPU throughput
			 * each group needs to keep up with the current workload.
			 */
			total_im_cycles
				+= (double)demand->produced_rate * cost->cost_apply;
			total_im_cycles
				+= (double)demand->completed_rate * cost->cost_convert;
			total_flush_cycles
				+= (double)demand->flushable_batch_rate * cost->cost_batch_flush;
		}
	}

	/*
	 * Add trade flush cycle needs using the rate-based model
	 * so that the units match the candle-pipeline stages
	 * (cycles/sec). Both trade_block_fill_rates and
	 * trade_flush_costs are per-symbol arrays indexed by
	 * sym_idx alone.
	 */
	for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
		total_flush_cycles +=
			(double)admin->trade_block_fill_rates[sym_idx]
			* admin->trade_flush_costs[sym_idx];
	}

	/*
	 * Book pipeline cycle needs.
	 *   - book_update: IM group, driven by event
	 *     production rate (events/sec from produced_count).
	 *   - book_event_flush: Flush group, driven by
	 *     unflushed block rate (blocks/sec).
	 */
	if (cache->book_enabled) {
		for (int sym_idx = 0; sym_idx < num_symbols;
				sym_idx++) {
			total_im_cycles +=
				(double)admin->book_update_rates
					[sym_idx]
				* admin->book_update_costs[sym_idx];
			total_flush_cycles +=
				(double)admin->book_block_fill_rates
					[sym_idx]
				* admin->book_event_flush_costs
					[sym_idx];
		}
	}

	*out_in_memory_cycles = total_im_cycles;
	*out_flush_cycles = total_flush_cycles;
}

/**
 * @brief   Determines worker counts for In-Memory and Flush groups.
 *
 * @param   num_workers:           Total workers available.
 * @param   total_im_cycles:       Total cycles needed for Apply+Convert.
 * @param   total_flush_cycles:    Total cycles needed for Flush group.
 * @param   out_num_im_workers:    (out) Number of workers for In-Memory.
 * @param   out_num_flush_workers: (out) Number of workers for Flush group.
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
		/*
		 * Proportional allocation:
		 *   num_im_workers = ceil(im_cycles / total_cycles × num_workers)
		 *
		 * ceil() biases toward the In-Memory group, which is
		 * latency-sensitive (Apply/Convert touch live candle data).
		 * Over-allocating one worker to in-memory work is preferable
		 * to under-allocating and stalling the hot path.
		 */
		*out_num_im_workers = (int)ceil(
			(total_im_cycles / total_cycles) * num_workers);
		*out_num_flush_workers = num_workers - *out_num_im_workers;

		/*
		 * Guarantee at least 1 worker per active group so that no
		 * pipeline starves even under extreme workload imbalance.
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
	 * Advance to the next worker when the current budget is exhausted,
	 * but only if there is another worker available in the group.
	 * The last worker absorbs all remaining tasks regardless of budget —
	 * every task must be assigned to someone, and we must not drop work.
	 */
	int max_worker_idx_in_group = state->start_worker_idx +
		state->num_workers_in_group - 1;

	if (state->current_budget < task_cost &&
		state->current_worker_idx < max_worker_idx_in_group)
	{
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
 * @brief   Assign Apply/Convert/Flush tasks for all candle pipelines.
 *
 * Iterates (type → symbol) for cache locality and distributes each of
 * the three candle-pipeline tasks across the appropriate budget group.
 *
 * @param   cache:       Global cache instance.
 * @param   admin:       Admin state containing demand and cost stats.
 * @param   symtab:      Symbol table.
 * @param   im_state:    Mutable budget state for the In-Memory group.
 * @param   flush_state: Mutable budget state for the Flush group.
 */
static void assign_candle_tasks(struct trcache *cache,
	struct admin_state *admin, struct symbol_table *symtab,
	struct budget_assign_state *im_state,
	struct budget_assign_state *flush_state)
{
	int num_symbols = atomic_load_explicit((int*)&symtab->num_symbols,
		memory_order_acquire);
	int max_syms = symtab->capacity;
	size_t in_mem_words = admin->in_mem_words_per_worker;
	size_t batch_flush_words = admin->batch_flush_words_per_worker;

	for (int type_idx = 0; type_idx < cache->num_candle_configs; type_idx++) {
		for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
			size_t task_idx = (size_t)CANDLE_TASK_IDX(
				type_idx, sym_idx, max_syms);
			struct sched_stage_rate *demand = &admin->stage_rates[task_idx];
			struct admin_task_costs *cost = &admin->task_costs[task_idx];

			/* Assign In-Memory (Apply) */
			double apply_task_cost =
				(double)demand->produced_rate * cost->cost_apply;
			assign_task_to_budget_group(im_state, apply_task_cost,
				admin->next_im_bitmaps, in_mem_words,
				IM_APPLY_BIT(task_idx));

			/* Assign In-Memory (Convert) */
			double convert_task_cost =
				(double)demand->completed_rate * cost->cost_convert;
			assign_task_to_budget_group(im_state, convert_task_cost,
				admin->next_im_bitmaps, in_mem_words,
				IM_CONVERT_BIT(task_idx));

			/* Assign Batch Flush (candle) */
			double batch_flush_task_cost =
				(double)demand->flushable_batch_rate * cost->cost_batch_flush;
			assign_task_to_budget_group(flush_state,
				batch_flush_task_cost,
				admin->next_batch_flush_bitmaps, batch_flush_words,
				task_idx);
		}
	}
}

/**
 * @brief   Assign trade-block flush tasks for all symbols.
 *
 * One bit per symbol (independent of candle type). Uses the same
 * flush_state budget so candle batch flush and trade flush work are
 * balanced across the same flush workers.
 *
 * Bits are assigned based on EMA fill rate alone, consistent with how
 * Apply/Convert/Batch Flush tasks are assigned. This keeps bitmap assignments
 * stable across admin cycles and lets the EMA decay naturally after a
 * burst rather than toggling bits on every cycle.
 * event_data_buffer_flush_full_blocks() handles the empty case
 * gracefully with an immediate return when no full blocks exist.
 *
 * @param   admin:       Admin state containing fill-rate and cost arrays.
 * @param   symtab:      Symbol table.
 * @param   flush_state: Mutable budget state for the Flush group.
 */
static void assign_trade_flush_tasks(struct admin_state *admin,
	struct symbol_table *symtab,
	struct budget_assign_state *flush_state)
{
	int num_symbols = atomic_load_explicit((int*)&symtab->num_symbols,
		memory_order_acquire);
	size_t trade_flush_words = admin->trade_flush_words_per_worker;

	for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
		struct event_data_buffer *tdb =
			symtab->symbol_entries[sym_idx].trd_buf;
		if (tdb == NULL) {
			continue;
		}
		double tf_cost =
			(double)admin->trade_block_fill_rates[sym_idx]
			* admin->trade_flush_costs[sym_idx];
		assign_task_to_budget_group(flush_state, tf_cost,
			admin->next_trade_flush_bitmaps, trade_flush_words,
			TF_BIT(sym_idx));
	}
}

/**
 * @brief   Assign book update tasks for all symbols.
 *
 * One bit per symbol, assigned to the In-Memory budget
 * group. Uses book_update_rates (events/sec from
 * produced_count) as the demand signal.
 *
 * @param   admin:    Admin state containing cost arrays.
 * @param   symtab:   Symbol table.
 * @param   im_state: Mutable budget state for IM group.
 */
static void assign_book_update_tasks(
	struct admin_state *admin,
	struct symbol_table *symtab,
	struct budget_assign_state *im_state)
{
	int num_symbols = atomic_load_explicit(
		(int *)&symtab->num_symbols,
		memory_order_acquire);
	size_t words =
		admin->book_update_words_per_worker;

	for (int sym_idx = 0; sym_idx < num_symbols;
			sym_idx++) {
		double cost =
			(double)admin->book_update_rates
				[sym_idx]
			* admin->book_update_costs[sym_idx];
		assign_task_to_budget_group(
			im_state, cost,
			admin->next_book_update_bitmaps,
			words, BU_BIT(sym_idx));
	}
}

/**
 * @brief   Assign book event flush tasks for all symbols.
 *
 * One bit per symbol, assigned to the Flush budget group.
 *
 * @param   admin:       Admin state containing cost arrays.
 * @param   symtab:      Symbol table.
 * @param   flush_state: Mutable budget state for Flush group.
 */
static void assign_book_event_flush_tasks(
	struct admin_state *admin,
	struct symbol_table *symtab,
	struct budget_assign_state *flush_state)
{
	int num_symbols = atomic_load_explicit(
		(int *)&symtab->num_symbols,
		memory_order_acquire);
	size_t words =
		admin->book_event_flush_words_per_worker;

	for (int sym_idx = 0; sym_idx < num_symbols;
			sym_idx++) {
		double cost =
			(double)admin->book_block_fill_rates
				[sym_idx]
			* admin->book_event_flush_costs
				[sym_idx];
		assign_task_to_budget_group(
			flush_state, cost,
			admin->next_book_event_flush_bitmaps,
			words, BF_BIT(sym_idx));
	}
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
 * @param   total_im_cycles:    Total cycle budget for In-Memory group.
 * @param   total_flush_cycles: Total cycle budget for Flush group.
 */
static void assign_tasks_to_bitmaps(struct trcache *cache,
	struct admin_state *admin, struct symbol_table *symtab,
	int num_im_workers, int num_flush_workers,
	double total_im_cycles, double total_flush_cycles)
{
	size_t in_mem_words = admin->in_mem_words_per_worker;
	size_t batch_flush_words =
		admin->batch_flush_words_per_worker;
	size_t trade_flush_words =
		admin->trade_flush_words_per_worker;
	size_t book_words =
		admin->book_update_words_per_worker;

	/* Clear the 'next' buffers for recalculation */
	memset(admin->next_im_bitmaps, 0,
		cache->num_workers * in_mem_words
			* sizeof(uint64_t));
	memset(admin->next_batch_flush_bitmaps, 0,
		cache->num_workers * batch_flush_words
			* sizeof(uint64_t));
	memset(admin->next_trade_flush_bitmaps, 0,
		cache->num_workers * trade_flush_words
			* sizeof(uint64_t));
	memset(admin->next_book_update_bitmaps, 0,
		cache->num_workers * book_words
			* sizeof(uint64_t));
	memset(admin->next_book_event_flush_bitmaps, 0,
		cache->num_workers * book_words
			* sizeof(uint64_t));

	/* Budget state for In-Memory group (workers 0..im-1) */
	struct budget_assign_state im_state = {
		.budget_per_worker = (num_im_workers > 0) ?
			total_im_cycles / num_im_workers : 0,
		.current_worker_idx = 0,
		.num_workers_in_group = num_im_workers,
		.start_worker_idx = 0
	};
	im_state.current_budget = im_state.budget_per_worker;

	/* Budget state for Flush group (workers im..end) */
	struct budget_assign_state flush_state = {
		.budget_per_worker = (num_flush_workers > 0) ?
			total_flush_cycles / num_flush_workers : 0,
		.current_worker_idx = num_im_workers,
		.num_workers_in_group = num_flush_workers,
		.start_worker_idx = num_im_workers
	};
	flush_state.current_budget = flush_state.budget_per_worker;

	assign_candle_tasks(
		cache, admin, symtab, &im_state, &flush_state);

	if (cache->trade_flush_ops.flush != NULL) {
		assign_trade_flush_tasks(
			admin, symtab, &flush_state);
	}

	if (cache->book_enabled) {
		assign_book_update_tasks(
			admin, symtab, &im_state);
		assign_book_event_flush_tasks(
			admin, symtab, &flush_state);
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
static void publish_new_assignments_to_workers(
	struct trcache *cache,
	struct admin_state *admin, int num_im_workers)
{
	size_t im_bytes =
		admin->in_mem_words_per_worker
			* sizeof(uint64_t);
	size_t bf_bytes =
		admin->batch_flush_words_per_worker
			* sizeof(uint64_t);
	size_t tf_bytes =
		admin->trade_flush_words_per_worker
			* sizeof(uint64_t);
	size_t bu_bytes =
		admin->book_update_words_per_worker
			* sizeof(uint64_t);
	size_t bef_bytes =
		admin->book_event_flush_words_per_worker
			* sizeof(uint64_t);

	for (int i = 0; i < cache->num_workers; i++) {
		struct worker_state *ws =
			&cache->worker_state_arr[i];
		int old_group = atomic_load_explicit(
			&ws->group_id,
			memory_order_acquire);

		if (i < num_im_workers) {
			/* In-Memory worker */
			int new_group = GROUP_IN_MEMORY;
			size_t im_w =
				admin->in_mem_words_per_worker;
			size_t bu_w =
				admin->book_update_words_per_worker;

			uint64_t *next_im =
				admin->next_im_bitmaps
				+ (i * im_w);
			uint64_t *cur_im =
				admin->current_im_bitmaps
				+ (i * im_w);
			uint64_t *next_bu =
				admin->next_book_update_bitmaps
				+ (i * bu_w);
			uint64_t *cur_bu =
				admin->current_book_update_bitmaps
				+ (i * bu_w);

			bool group_changed =
				(old_group != new_group);
			bool bitmaps_changed =
				memcmp(next_im, cur_im,
					im_bytes) != 0 ||
				memcmp(next_bu, cur_bu,
					bu_bytes) != 0;

			if (!group_changed && !bitmaps_changed) {
				continue;
			}

			/* Copy new bitmaps to worker */
			memcpy(ws->in_memory_bitmap,
				next_im, im_bytes);
			memcpy(ws->book_update_bitmap,
				next_bu, bu_bytes);

			/* Save for next comparison */
			memcpy(cur_im, next_im, im_bytes);
			memcpy(cur_bu, next_bu, bu_bytes);

			if (group_changed) {
				/*
				 * Clear admin's current copies
				 * for the opposite (Flush) group
				 * to prevent spurious change
				 * detection on future cycles.
				 */
				size_t bf_w = admin
					->batch_flush_words_per_worker;
				size_t tf_w = admin
					->trade_flush_words_per_worker;
				size_t bef_w = admin
					->book_event_flush_words_per_worker;
				memset(
					admin->current_batch_flush_bitmaps
					+ (i * bf_w), 0, bf_bytes);
				memset(
					admin->current_trade_flush_bitmaps
					+ (i * tf_w), 0, tf_bytes);
				memset(
					admin->current_book_event_flush_bitmaps
					+ (i * bef_w), 0, bef_bytes);
			}

			/*
			 * Release store last: all bitmap
			 * writes above become visible when
			 * the worker acquire-loads group_id.
			 */
			atomic_store_explicit(
				&ws->group_id, new_group,
				memory_order_release);

		} else {
			/* Flush worker */
			int new_group = GROUP_FLUSH;
			size_t bf_w =
				admin->batch_flush_words_per_worker;
			size_t tf_w =
				admin->trade_flush_words_per_worker;
			size_t bef_w = admin
				->book_event_flush_words_per_worker;

			uint64_t *next_bf =
				admin->next_batch_flush_bitmaps
				+ (i * bf_w);
			uint64_t *cur_bf =
				admin->current_batch_flush_bitmaps
				+ (i * bf_w);
			uint64_t *next_tf =
				admin->next_trade_flush_bitmaps
				+ (i * tf_w);
			uint64_t *cur_tf =
				admin->current_trade_flush_bitmaps
				+ (i * tf_w);
			uint64_t *next_bef =
				admin->next_book_event_flush_bitmaps
				+ (i * bef_w);
			uint64_t *cur_bef =
				admin->current_book_event_flush_bitmaps
				+ (i * bef_w);

			bool group_changed =
				(old_group != new_group);
			bool bitmaps_changed =
				memcmp(next_bf, cur_bf,
					bf_bytes) != 0 ||
				memcmp(next_tf, cur_tf,
					tf_bytes) != 0 ||
				memcmp(next_bef, cur_bef,
					bef_bytes) != 0;

			if (!group_changed && !bitmaps_changed) {
				continue;
			}

			/* Copy new bitmaps to worker */
			memcpy(ws->batch_flush_bitmap,
				next_bf, bf_bytes);
			memcpy(ws->trade_flush_bitmap,
				next_tf, tf_bytes);
			memcpy(ws->book_event_flush_bitmap,
				next_bef, bef_bytes);

			/* Save for next comparison */
			memcpy(cur_bf, next_bf, bf_bytes);
			memcpy(cur_tf, next_tf, tf_bytes);
			memcpy(cur_bef, next_bef, bef_bytes);

			if (group_changed) {
				/*
				 * Clear admin's current copies
				 * for the opposite (IM) group
				 * to prevent spurious change
				 * detection on future cycles.
				 */
				size_t im_w = admin
					->in_mem_words_per_worker;
				size_t bu_w = admin
					->book_update_words_per_worker;
				memset(
					admin->current_im_bitmaps
					+ (i * im_w), 0, im_bytes);
				memset(
					admin->current_book_update_bitmaps
					+ (i * bu_w), 0, bu_bytes);
			}

			/*
			 * Release store last: all bitmap
			 * writes above become visible when
			 * the worker acquire-loads group_id.
			 */
			atomic_store_explicit(
				&ws->group_id, new_group,
				memory_order_release);
		}
	}
}

/**
 * @brief   Balance work assignments across workers.
 *
 * This is the main scheduler function.
 * 1. Calculates total cycle requirements for (In-Memory) vs (Flush).
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
 *          the global memory_pressure flag.
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

		/* Sum trade buffer memory */
		if (entry->trd_buf) {
			total_mem += mem_get_atomic(
				&entry->trd_buf->memory_usage.value);
		}

		/* Sum book buffer memory */
		if (entry->book_buf) {
			total_mem += mem_get_atomic(
				&entry->book_buf->memory_usage.value);
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
		/* 1. Update demand statistics */
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
