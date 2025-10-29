/**
 * @file   admin_thread.c
 * @brief  Implementation of the admin thread main routine.
 */
#define _GNU_SOURCE
#include <math.h>
#include <sched.h>
#include <string.h>
#include <time.h>

#include "sched/admin_thread.h"
#include "meta/symbol_table.h"
#include "sched/sched_pipeline_stats.h"
#include "sched/worker_thread.h"
#include "concurrent/atomsnap.h"
#include "utils/log.h"
#include "utils/tsc_clock.h"

/*
 * Definition of the global timestamp maintained by the admin thread.
 * See admin_thread.h for the corresponding declaration.
 */
_Atomic uint64_t g_admin_current_ts_ms = 0;

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

	if (!tc) {
		errmsg(stderr, "Invalid trcache pointer\n");
		return -1;
	}

	state = &(tc->admin_state);
	state->sched_msg_queue = scq_init(&tc->mem_acc);
	if (state->sched_msg_queue == NULL) {
		errmsg(stderr, "admin sched_msg_queue allocation failed\n");
		return -1;
	}
	state->done = false;

	return 0;
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

	scq_destroy(state->sched_msg_queue);
	state->sched_msg_queue = NULL;
}

/**
 * @brief   Update pipeline statistics for all symbols.
 *
 * @param   cache:  Global cache instance.
 */
void update_all_pipeline_stats(struct trcache *cache)
{
	struct symbol_table *table = cache->symbol_table;
	struct atomsnap_version *ver = NULL;
	struct symbol_entry **arr = NULL;
	int num = 0;

	ver = atomsnap_acquire_version(table->symbol_ptr_array_gate);
	arr = (struct symbol_entry **)ver->object;
	num = table->num_symbols;

	for (int i = 0; i < num; i++) {
		sched_pipeline_calc_rates(cache, arr[i]);
	}

	atomsnap_release_version(ver);
}

/**
 * @brief   Compute average worker throughput per stage.
 *
 * @param   cache:  Global cache instance.
 * @param   out:    Array indexed by stage, filled with items per second.
 */
static void compute_worker_speeds(struct trcache *cache, double *out)
{
	double hz = tsc_cycles_per_sec();

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		uint64_t cycles = 0;
		uint64_t count = 0;

		for (int w = 0; w < cache->num_workers; w++) {
			struct worker_stat_board *b = &cache->worker_state_arr[w].stat;

			for (int i = 0; i < cache->num_candle_configs; i++) {
				if (s == WORKER_STAT_STAGE_APPLY) {
					cycles += b->apply_stat[i].cycles;
					count += b->apply_stat[i].work_count;
				} else if (s == WORKER_STAT_STAGE_CONVERT) {
					cycles += b->convert_stat[i].cycles;
					count += b->convert_stat[i].work_count;
				} else { // FLUSH
					cycles += b->flush_stat[i].cycles;
					count += b->flush_stat[i].work_count;
				}
			}
		}
		
		if (cycles != 0) {
			out[s] = (double)count * hz / (double)cycles;
		} else if (count > 0){
			/* fallback when cycles==0 but work was done */
			out[s] = (double)count;
		} else {
			out[s] = 0.0;
		}
	}
}

/**
 * @brief   Aggregate pipeline throughput across all symbols.
 *
 * @param   cache:  Global cache instance.
 * @param   out:    Array indexed by stage, filled with items per second.
 */
static void compute_pipeline_demand(struct trcache *cache, double *out)
{
	struct symbol_table *table = cache->symbol_table;
	struct atomsnap_version *ver
		= atomsnap_acquire_version(table->symbol_ptr_array_gate);
	struct symbol_entry **arr = (struct symbol_entry **)ver->object;
	int num = table->num_symbols;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		out[s] = 0.0;
	}

	for (int i = 0; i < num; i++) {
		struct symbol_entry *e = arr[i];

		for (int j = 0; j < cache->num_candle_configs; j++) {
			struct sched_stage_rate *r = &e->pipeline_stats.stage_rates[j];

			out[WORKER_STAT_STAGE_APPLY] += (double)r->produced_rate;
			out[WORKER_STAT_STAGE_CONVERT] += (double)r->completed_rate;
			out[WORKER_STAT_STAGE_FLUSH] += (double)r->flushable_batch_rate;
		}
	}

	atomsnap_release_version(ver);
}

/**
 * @brief   Clear worker bitmasks.
 */
static void reset_stage_ct_masks(struct trcache *cache)
{
	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		for (int w = 0; w < cache->num_workers; w++) {
			cache->stage_ct_mask[s][w] = 0;
		}
	}
}
	
/**
 * @brief   Send a work message to a worker.
 *
 * @param   cache:      Global cache instance.
 * @param   worker_id:  Destination worker index.
 * @param   candle_idx: Candle type index.
 * @param   stage:      Pipeline stage.
 * @param   symbol_id:  Target symbol ID.
 * @param   kind:       Message type to send.
 */
static void post_work_msg(struct trcache *cache, int worker_id,
       int candle_idx, worker_stat_stage_type stage,
       int symbol_id, enum sched_msg_type kind)
{
	struct worker_state *state = &cache->worker_state_arr[worker_id];
	struct sched_work_msg *msg =
		sched_work_msg_alloc(cache->sched_msg_free_list);

	if (msg == NULL) {
		return;
	}

	msg->cmd.symbol_id = symbol_id;
	msg->cmd.stage = stage;
	msg->cmd.candle_idx = candle_idx;
	msg->type = kind;

	sched_post_work_msg(state->sched_msg_queue, msg);
}

/**
 * @brief   Choose the worker with the lowest adjusted load.
 *
 * Each load entry is penalised when the worker is not currently handling
 * the candle type according to its mask.
 *
 * @param   load:       Array of per-worker load counters.
 * @param   limit:      Number of valid entries in @load.
 * @param   mask:       Per-worker bitmask for the stage.
 * @param   candle_idx: Candle type index being scheduled.
 *
 * @return  Index of the least-loaded worker.
 */
static int choose_best_worker(double *load, int limit,
	uint32_t *mask, int candle_idx)
{
	const double PENALTY = 10.0;
	int best = 0;
	double best_score = load[0] +
		((mask[0] & (1u << candle_idx)) ? 0.0 : PENALTY);
	
	for (int w = 1; w < limit; w++) {
		double score = load[w] +
			((mask[w] & (1u << candle_idx)) ? 0.0 : PENALTY);
		if (score < best_score) {
			best_score = score;
			best = w;
		}
	}
	
	return best;
}

/*
 * stage_sched_env - Parameters for scheduling a pipeline stage.
 *
 * @stage:  Pipeline stage identifier.
 * @load:   Pointer to the per-worker load array for this stage.
 * @limit:  Number of workers that may handle this stage.
 * @start:  Index of the first worker assigned to this stage.
 */
struct stage_sched_env {
	worker_stat_stage_type stage;
	double *load;
	int limit;
	int start;
};

/**
 * @brief   Update the worker assignment for a symbol stage.
 *
 * If the stage is already assigned to @worker, nothing is done. Otherwise a
 * remove message is sent to the current worker (if any) and an add message is
 * posted to the new worker.
 *
 * @param   cache:      Global cache instance.
 * @param   entry:      Target symbol entry.
 * @param   candle_idx: Candle type identifier.
 * @param   env:        Scheduling environment for the stage.
 * @param   worker:     Destination worker index.
 */
static void update_stage_assignment(struct trcache *cache,
	struct symbol_entry *entry, int candle_idx,
	struct stage_sched_env *env, int worker)
{
	int cur = atomic_load(&entry->in_progress[env->stage][candle_idx]);
	if (cur == worker) {
		return;
	}
				
	if (cur >= 0) {
		/* remove candle index from previous worker bitmask */
		cache->stage_ct_mask[env->stage][cur] &= ~(1u << candle_idx);
		post_work_msg(cache, cur, candle_idx, env->stage,
			entry->id, SCHED_MSG_REMOVE_WORK);
	}
	
	post_work_msg(cache, worker, candle_idx, env->stage,
		entry->id, SCHED_MSG_ADD_WORK);
	
	/* assign candle index to new worker bitmask */
	cache->stage_ct_mask[env->stage][worker] |= (1u << candle_idx);
}

/**
 * @brief   Distribute demand for a single stage across workers.
 *
 * Chooses the least loaded worker, updates its load counter and adjusts the
 * worker assignment accordingly.
 *
 * @param   cache:       Global cache instance.
 * @param   entry:       Target symbol entry.
 * @param   candle_idx:  Candle type identifier.
 * @param   demand:      Estimated demand for this stage.
 * @param   env:         Scheduling environment describing stage limits.
 */
static void schedule_symbol_stage(struct trcache *cache,
	struct symbol_entry *entry,
	int candle_idx, double demand,
	struct stage_sched_env *env)
{
	if (env->limit <= 0) {
		int cur = atomic_load(&entry->in_progress[env->stage][candle_idx]);
		if (cur >= 0) {
			/* remove candle index from previous worker bitmask */
			cache->stage_ct_mask[env->stage][cur] &= ~(1u << candle_idx);
			post_work_msg(cache, cur, candle_idx, env->stage,
				entry->id, SCHED_MSG_REMOVE_WORK);
		}
		return;
	}
	
	int best = choose_best_worker(env->load, env->limit,
		cache->stage_ct_mask[env->stage], candle_idx) + env->start;
	env->load[best - env->start] += demand;
	
	update_stage_assignment(cache, entry, candle_idx, env, best);
}
	
/**
 * @brief   Schedule work for all stages of a symbol.
 *
 * Initialises per-stage scheduling environments and distributes demand for each
 * candle type across workers.
 *
 * @param   cache:         Global cache instance.
 * @param   entry:         Target symbol entry.
 * @param   load:          Two-dimensional array storing load per stage/worker.
 * @param   stage_limits:  Maximum number of workers allowed per stage.
 * @param   stage_start:   Index of the first worker allocated to each stage.
 */
static void schedule_symbol_work(struct trcache *cache,
	struct symbol_entry *entry, double load[][MAX_NUM_THREADS],
	const int *stage_limits, const int *stage_start)
{
	struct stage_sched_env env[WORKER_STAT_STAGE_NUM];
	double demand[WORKER_STAT_STAGE_NUM];
	struct sched_stage_rate *stage_rate;
	
	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		env[s].stage = s;
		env[s].load = load[s];
		env[s].limit = stage_limits[s];
		env[s].start = stage_start[s];
	}

	for (int i = 0; i < cache->num_candle_configs; i++) {
		stage_rate = &entry->pipeline_stats.stage_rates[i];

		demand[WORKER_STAT_STAGE_APPLY]
			= (double)stage_rate->produced_rate + 1.0;
		demand[WORKER_STAT_STAGE_CONVERT]
			= (double)stage_rate->completed_rate + 1.0;
		demand[WORKER_STAT_STAGE_FLUSH]
			= (double)stage_rate->flushable_batch_rate + 1.0;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			schedule_symbol_stage(cache, entry, i, demand[s], &env[s]);
		}
	}
}

/**
 * @brief   Calculate initial worker needs per stage based on demand/speed.
 *
 * @param   speed:            Array of worker speeds per stage.
 * @param   demand:           Array of pipeline demand per stage.
 * @param   initial_need_out: Output array for initial needs.
 *
 * @return  Total initial needed workers across all stages.
 */
static int calculate_initial_needs(const double *speed,
	const double *demand, int *initial_need_out)
{
	int total_initial_need = 0;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		if (speed[s] > 0) {
			initial_need_out[s] = (int)ceil(demand[s] / speed[s]);
		} else if (demand[s] > 0) {
			initial_need_out[s] = 1;
		} else {
			initial_need_out[s] = 0;
		}
		total_initial_need += initial_need_out[s];
	}

	return total_initial_need;
}

/**
 * @brief   Calculate precise needs and total for proportional allocation.
 *
 * @param   speed:                  Array of worker speeds per stage.
 * @param   demand:                 Array of pipeline demand per stage.
 * @param   initial_need:           Array of initial needs (used if speed=0).
 * @param   stage_precise_need_out: Output array for precise needs.
 *
 * @return  Total precise need across all stages.
 */
static double calculate_proportional_needs(const double *speed,
	const double *demand, const int* initial_need,
	double *stage_precise_need_out)
{
	double total_precise_need = 0.0;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; ++s) {
		if (speed[s] > 0) {
			stage_precise_need_out[s] = demand[s] / speed[s];
		} else if (demand[s] > 0) {
			stage_precise_need_out[s] = (double)initial_need[s];
		} else {
			stage_precise_need_out[s] = 0.0;
		}
		total_precise_need += stage_precise_need_out[s];
	}

	return total_precise_need;
}

/**
 * @brief   Distribute remaining workers based on largest fractional needs.
 *
 * @param   num_workers:     Total available workers.
 * @param   total_allocated: Workers allocated in the integer phase.
 * @param   need_fraction:   Array of fractional needs remaining.
 * @param   limits_out:      Output array for final worker limits.
 */
static void distribute_remaining_workers(int num_workers, int total_allocated,
	double *need_fraction, int *limits_out)
{
	int remaining = num_workers - total_allocated;

	while (remaining > 0) {
		int best_stage = -1;
		double max_fraction = -1.0;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			if (need_fraction[s] > max_fraction) {
				max_fraction = need_fraction[s];
				best_stage = s;
			}
		}

		if (best_stage == -1 || max_fraction <= 0) {
			for (int s_fb = 0; s_fb < WORKER_STAT_STAGE_NUM; s_fb++) {
				int current_total = 0;

				for(int sc = 0; sc < WORKER_STAT_STAGE_NUM; sc++) {
					current_total += limits_out[sc];
				}

				if (current_total < num_workers) {
					limits_out[s_fb]++;
					if (--remaining == 0) {
						break;
					}
				} else {
					remaining = 0;
					break;
				}
			}

			if (remaining > 0) {
				errmsg(stderr, "Failed to allocate remain workers\n");
				break;
			}
		} else {
			limits_out[best_stage]++;
			need_fraction[best_stage] = -1.0;
			remaining--;
		}
	}
}

/**
 * @brief   Allocate integer part of workers based on proportional needs.
 *
 * @param   num_workers:        Total available workers.
 * @param   precise_total_need: Sum of precise needs.
 * @param   stage_precise_need: Array of precise needs per stage.
 * @param   need_fraction_out:  Output array for fractional remainders.
 * @param   limits_out:         Output array for integer allocated limits.
 *
 * @return  Total workers allocated in this phase.
 */
static int allocate_integer_part(int num_workers, double precise_total_need,
	const double* stage_precise_need, double* need_fraction_out,
	int* limits_out)
{
	int total_allocated = 0;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		double ratio = stage_precise_need[s] / precise_total_need;
		double alloc_double = (double)num_workers * ratio;
		limits_out[s] = (int)floor(alloc_double);
		need_fraction_out[s] = alloc_double - limits_out[s];
		total_allocated += limits_out[s];
	}

	return total_allocated;
}

/**
 * @brief   Allocate workers proportionally when need > available.
 *
 * @param   num_workers:  Total available workers.
 * @param   speed:        Array of worker speeds per stage.
 * @param   demand:       Array of pipeline demand per stage.
 * @param   initial_need: Array of initial needs (used if speed=0).
 * @param   limits_out:   Output array for final worker limits.
 */
static void allocate_workers_proportionally(int num_workers,
	const double *speed, const double *demand, const int* initial_need,
	int *limits_out)
{
	double stage_precise_need[WORKER_STAT_STAGE_NUM] = {0.0};
	double need_fraction[WORKER_STAT_STAGE_NUM] = {0.0};
	int total_allocated = 0;
	double precise_total_need;

	precise_total_need = calculate_proportional_needs(speed, demand,
		initial_need, stage_precise_need);

	if (precise_total_need <= 0) {
		int wps = num_workers / WORKER_STAT_STAGE_NUM;
		int rem = num_workers % WORKER_STAT_STAGE_NUM;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			limits_out[s] = wps + (s < rem ? 1 : 0);
			total_allocated += limits_out[s];
		}

		if (total_allocated != num_workers && WORKER_STAT_STAGE_NUM > 0) {
			 limits_out[0] += (num_workers - total_allocated);
		}

		return;
    }

	total_allocated = allocate_integer_part(num_workers, precise_total_need,
		stage_precise_need, need_fraction, limits_out);

	distribute_remaining_workers(num_workers, total_allocated,
		need_fraction, limits_out);
}


/**
 * @brief   Allocate idle workers when need <= available.
 *
 * @param   num_workers:        Total available workers.
 * @param   total_initial_need: Sum of initial needs.
 * @param   initial_need:       Array of initial needs per stage.
 * @param   speed:              Array of worker speeds per stage.
 * @param   demand:             Array of pipeline demand per stage.
 * @param   limits_out:         Output array for final worker limits.
 */
static void allocate_idle_workers(int num_workers, int total_initial_need,
	const int *initial_need, const double *speed, const double *demand,
	int *limits_out)
{
	int remaining_workers = num_workers - total_initial_need;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		limits_out[s] = initial_need[s];
	}

	if (remaining_workers > 0) {
		int best_stage = -1;
		double best_ratio = -1.0;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			double ratio = 0.0;
			if (speed[s] > 0) {
				ratio = demand[s] / speed[s];
			} else if (demand[s] > 0) {
				ratio = 1e12;
			}

			if (ratio > best_ratio) {
				best_ratio = ratio;
				best_stage = s;
			}
		}

		if (best_stage == -1) {
			best_stage = WORKER_STAT_STAGE_APPLY;
		}

		limits_out[best_stage] += remaining_workers;
	}
}

/**
 * @brief   Ensure stages with demand have at least one worker and
 *          adjust limits.
 *
 * This function guarantees that any stage with non-zero demand receives at
 * least one worker. If adding these minimum workers causes the total allocated
 * workers to exceed the available number, it iteratively removes workers from
 * the stages with the highest allocation (avoiding stages that only have their
 * guaranteed minimum) until the total matches the available count.
 *
 * @param   num_workers: Total number of available worker threads.
 * @param   demand:      Array of estimated demand per stage.
 * @param   limits_out:  Input/Output array of worker limits per stage. This
 *                       array is modified in place.
 */
static void ensure_minimum_worker_allocation(int num_workers,
	const double *demand, int *limits_out)
{
	int guaranteed_workers = 0;
	int current_total_workers = 0;
	int excess_workers = 0;

	/* Ensure minimum allocation for stages with demand */
	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		if (demand[s] > 0.0 && limits_out[s] == 0) {
			limits_out[s] = 1;
			guaranteed_workers++;
		}
	}

	/* If minimum guarantees were applied, check for excess workers */
	if (guaranteed_workers > 0) {
		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			current_total_workers += limits_out[s];
		}

		excess_workers = current_total_workers - num_workers;

		/* Reduce workers from stages with the highest limits if necessary */
		while (excess_workers > 0) {
			int max_limit = 0;
			int max_stage = -1;
			bool can_reduce = false;

			/*
			 * First pass: Find the stage with the most workers, excluding
			 * those that only have the guaranteed minimum of 1.
			 */
			for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
				bool is_guaranteed_minimum
					= (demand[s] > 0.0 && limits_out[s] == 1);
				if (!is_guaranteed_minimum && limits_out[s] > max_limit) {
					max_limit = limits_out[s];
					max_stage = s;
					can_reduce = true; // Found a stage to reduce from
				}
			}

			/*
			 * Second pass (fallback): If no stage could be reduced in the first
			 * pass (e.g., all demanding stages have exactly 1 worker),
			 * find any stage with more than 1 worker, regardless of demand.
			 */
			if (!can_reduce) {
				max_limit = 1; // Reset max_limit check for this pass
				for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
					if (limits_out[s] > max_limit) {
						max_limit = limits_out[s];
						max_stage = s;
						can_reduce = true;
					}
				}
			}

			/* Reduce worker count from the selected stage */
			if (can_reduce && max_stage != -1) {
				limits_out[max_stage]--;
				excess_workers--;
			} else {
				errmsg(stderr,
					"Could not reduce excess workers (%d) while "
					"guaranteeing minimum allocation.\n", excess_workers);
				break; /* Exit loop to avoid infinite loop */
			}
		}
	}
}

/**
 * @brief   Final check and correction for worker limits.
 *
 * @param   num_workers: Total available workers.
 * @param   limits:      Array of calculated limits to finalize.
 */
static void finalize_limits(int num_workers, int *limits)
{
	int final_total_check = 0;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; ++s) {
		if (limits[s] < 0) {
			limits[s] = 0;
		}
		final_total_check += limits[s];
	}

	if (final_total_check != num_workers) {
		int diff = num_workers - final_total_check;
		errmsg(stderr,
			"Limit mismatch: total %d != needed %d. Adjust by %d.\n",
			final_total_check, num_workers, diff);
		return;
	}
}

/**
 * @brief   Estimate worker limits per stage based on demand and speed.
 *
 * @param   cache:  Global cache instance.
 * @param   limits: Output array (size WORKER_STAT_STAGE_NUM) for limits.
 */
void compute_stage_limits(struct trcache *cache, int *limits)
{
	double speed[WORKER_STAT_STAGE_NUM] = { 0.0 };
	double demand[WORKER_STAT_STAGE_NUM] = { 0.0 };
	int initial_need[WORKER_STAT_STAGE_NUM] = { 0 };
	int total_initial_need_int = 0;

	compute_worker_speeds(cache, speed);
	compute_pipeline_demand(cache, demand);

	total_initial_need_int = calculate_initial_needs(speed, demand,
		initial_need);

	if (total_initial_need_int > cache->num_workers) {
		allocate_workers_proportionally(cache->num_workers, speed, demand,
			initial_need, limits);
	} else {
		allocate_idle_workers(cache->num_workers, total_initial_need_int,
			initial_need, speed, demand, limits);
	}

	/* Ensure stages with demand get at least one worker, adjust if needed */
	ensure_minimum_worker_allocation(cache->num_workers, demand, limits);

	finalize_limits(cache->num_workers, limits);
}

/**
 * @brief   Calculate starting worker index for each stage sequentially.
 *
 * @param   cache:  Global cache instance.
 * @param   limits: Input array of worker limits per stage.
 * @param   start:  Output array (size WORKER_STAT_STAGE_NUM) for start indices.
 */
void compute_stage_starts(struct trcache *cache, int *limits, int *start)
{
	start[WORKER_STAT_STAGE_APPLY] = 0;
	start[WORKER_STAT_STAGE_CONVERT] =
		start[WORKER_STAT_STAGE_APPLY] + limits[WORKER_STAT_STAGE_APPLY];
	start[WORKER_STAT_STAGE_FLUSH] =
		start[WORKER_STAT_STAGE_CONVERT] + limits[WORKER_STAT_STAGE_CONVERT];

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; ++s) {
		if (start[s] < 0) {
			errmsg(stderr, "Warn: Negative start %d stage %d\n", start[s], s);
			start[s] = 0;
		}

		if (cache->num_workers > 0 && start[s] >= cache->num_workers
			&& limits[s] > 0) {
			errmsg(stderr, "Warn: Start %d >= workers %d stage %d\n",
					start[s], cache->num_workers, s);
			start[s] = cache->num_workers - 1;
		} else if (cache->num_workers == 0) {
			start[s] = 0;
		}

		if (start[s] + limits[s] > cache->num_workers) {
			errmsg(stderr, "Warn: Stage %d range [%d, %d) > workers %d\n",
					s, start[s], start[s] + limits[s], cache->num_workers);
			limits[s] = cache->num_workers - start[s];
			if (limits[s] < 0) {
				limits[s] = 0;
			}
		}
	}
}

/**
 * @brief   Balance work assignments across workers.
 *
 * @param   cache:  Global cache instance.
 */
static void balance_workers(struct trcache *cache)
{
	struct symbol_table *table = cache->symbol_table;
	struct atomsnap_version *ver = NULL;
	struct symbol_entry **arr = NULL;
	int num = 0;
	int limits[WORKER_STAT_STAGE_NUM] = { 0, };
	int stage_start[WORKER_STAT_STAGE_NUM] = { 0, };
	double load[WORKER_STAT_STAGE_NUM][MAX_NUM_THREADS] = { { 0, } };
	
	reset_stage_ct_masks(cache);

	compute_stage_limits(cache, limits);
	compute_stage_starts(cache, limits, stage_start);

	ver = atomsnap_acquire_version(table->symbol_ptr_array_gate);
	arr = (struct symbol_entry **)ver->object;
	num = table->num_symbols;

	for (int i = 0; i < num; i++) {
		schedule_symbol_work(cache, arr[i], load, limits, stage_start);
	}

	atomsnap_release_version(ver);
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
	struct timespec ts;
	uint64_t now_ms;

	while (!cache->admin_state.done) {
		if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
			now_ms = (uint64_t)ts.tv_sec * 1000ULL 
				+ (uint64_t)(ts.tv_nsec / 1000000ULL);
			atomic_store_explicit(&g_admin_current_ts_ms, now_ms,
				memory_order_release);
		}

		update_all_pipeline_stats(cache);
		balance_workers(cache);
		usleep((useconds_t)(ADMIN_THREAD_PERIOD_MS * 1000U));
	}

	return NULL;
}
