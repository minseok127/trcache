/**
 * @file   admin_thread.c
 * @brief  Implementation of the admin thread main routine.
 */
#define _GNU_SOURCE
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
	int idx;

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		uint64_t cycles = 0;
		uint64_t count = 0;

		for (int w = 0; w < cache->num_workers; w++) {
			struct worker_stat_board *b = &cache->worker_state_arr[w].stat;
			for (int i = 0; i < NUM_CANDLE_BASES; ++i) {
				for (int j = 0; j < cache->num_candle_types[i]; ++j) {
					if (s == WORKER_STAT_STAGE_APPLY) {
						cycles += b->apply_stat[i][j].cycles;
						count += b->apply_stat[i][j].work_count;
					} else if (s == WORKER_STAT_STAGE_CONVERT) {
						cycles += b->convert_stat[i][j].cycles;
						count += b->convert_stat[i][j].work_count;
					} else { // FLUSH
						cycles += b->flush_stat[i][j].cycles;
						count += b->flush_stat[i][j].work_count;
					}
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

		for (int j = 0; j < NUM_CANDLE_BASES; ++j) {
			for (int k = 0; k < cache->num_candle_types[j]; ++k) {
				struct sched_stage_rate *r
					= &e->pipeline_stats.stage_rates[j][k];

				out[WORKER_STAT_STAGE_APPLY] += (double)r->produced_rate;
				out[WORKER_STAT_STAGE_CONVERT] += (double)r->completed_rate;
				out[WORKER_STAT_STAGE_FLUSH] += (double)r->converted_rate;
			}
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
		memset(cache->stage_ct_mask[s], 0,
		sizeof(uint32_t) * cache->num_workers);
	}
}
	
/**
 * @brief   Send a work message to a worker.
 *
 * @param   cache:      Global cache instance.
 * @param   worker_id:  Destination worker index.
 * @param   type:       Candle type mask.
 * @param   stage:      Pipeline stage.
 * @param   symbol_id:  Target symbol ID.
 * @param   kind:       Message type to send.
 */
static void post_work_msg(struct trcache *cache, int worker_id,
       trcache_candle_type type, worker_stat_stage_type stage,
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
	msg->cmd.candle_type = type;
	msg->type = kind;

	sched_post_work_msg(state->sched_msg_queue, msg);
}

/**
 * @brief   Choose the worker with the lowest adjusted load.
 *
 * Each load entry is penalised when the worker is not currently handling
 * @candle_idx according to @mask.
 *
 * @param   load:       Array of per-worker load counters.
 * @param   limit:      Number of valid entries in @load.
 * @param   mask:       Per-worker candle-type bitmask for the stage.
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
		double score =
			load[w] + ((mask[w] & (1u << candle_idx)) ? 0.0 : PENALTY);
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
 * @param   cache:   Global cache instance.
 * @param   entry:   Target symbol entry.
 * @param   idx:     Candle type index.
 * @param   type:    Candle type mask.
 * @param   env:     Scheduling environment for the stage.
 * @param   worker:  Destination worker index.
 */
static void update_stage_assignment(struct trcache *cache,
	struct symbol_entry *entry, int idx,
	trcache_candle_type type, struct stage_sched_env *env,
	int worker)
{
	int cur = atomic_load(&entry->in_progress[env->stage][idx]);
	if (cur == worker) {
		return;
	}
				
	if (cur >= 0) {
		/* remove candle index from previous worker bitmask */
		cache->stage_ct_mask[env->stage][cur] &= ~(1u << idx);
		post_work_msg(cache, cur, type, env->stage,
			entry->id, SCHED_MSG_REMOVE_WORK);
	}
	
	post_work_msg(cache, worker, type, env->stage,
		entry->id, SCHED_MSG_ADD_WORK);
	
	/* assign candle index to new worker bitmask */
	cache->stage_ct_mask[env->stage][worker] |= (1u << idx);
}
	
/**
 * @brief   Distribute demand for a single stage across workers.
 *
 * Chooses the least loaded worker, updates its load counter and adjusts the
 * worker assignment accordingly.
 *
 * @param   cache:   Global cache instance.
 * @param   entry:   Target symbol entry.
 * @param   idx:     Candle type index.
 * @param   type:    Candle type mask.
 * @param   demand:  Estimated demand for this stage.
 * @param   env:     Scheduling environment describing stage limits.
 */
static void schedule_symbol_stage(struct trcache *cache,
	struct symbol_entry *entry, int idx,
	trcache_candle_type type, double demand,
	struct stage_sched_env *env)
{
	if (env->limit <= 0) {
		return;
	}
	
	int best = choose_best_worker(env->load, env->limit,
		cache->stage_ct_mask[env->stage], idx) + env->start;
	env->load[best - env->start] += demand;
	
	update_stage_assignment(cache, entry, idx, type, env, best);
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
	trcache_candle_type_flags flags = cache->candle_type_flags;
	struct sched_stage_rate *stage_rate;
	trcache_candle_type type;
	double demand[WORKER_STAT_STAGE_NUM];
	int idx;
	
	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		env[s].stage = s;
		env[s].load = load[s];
		env[s].limit = stage_limits[s];
		env[s].start = stage_start[s];
	}
	
	for (uint32_t m = flags; m != 0; m &= m - 1) {
		idx = __builtin_ctz(m);
		stage_rate = &entry->pipeline_stats.stage_rates[idx];
		demand[WORKER_STAT_STAGE_APPLY]
			= (double)stage_rate->produced_rate + 1.0;
		demand[WORKER_STAT_STAGE_CONVERT]
			= (double)stage_rate->completed_rate + 1.0;
		demand[WORKER_STAT_STAGE_FLUSH]
			= (double)stage_rate->converted_rate + 1.0;
		type = 1u << idx;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			schedule_symbol_stage(cache, entry, idx, type,
				demand[s], &env[s]);
		}
	}
}

/**
 * @brief   Estimate worker limits per pipeline stage.
 *
 * @param   cache:  Global cache instance.
 * @param   limits: Output array sized WORKER_STAT_STAGE_NUM.
 */
void compute_stage_limits(struct trcache *cache, int *limits)
{
	double speed[WORKER_STAT_STAGE_NUM] = { 0, };
	double demand[WORKER_STAT_STAGE_NUM] = { 0, };
	double ratio, best_ratio;
	int need, idle, total, best_stage;

	compute_worker_speeds(cache, speed);
	compute_pipeline_demand(cache, demand);

	for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
		need = 1;

		if (speed[s] > 0.0) {
			need = (int)((demand[s] / speed[s]) + 0.999);
		}

		if (need < 1) {
			need = 1;
		}

		limits[s] = need;
	}

	total = limits[WORKER_STAT_STAGE_APPLY] +
		limits[WORKER_STAT_STAGE_CONVERT] + limits[WORKER_STAT_STAGE_FLUSH];

	if (total >= cache->num_workers) {
		limits[WORKER_STAT_STAGE_APPLY] = cache->num_workers - 2;
		
		if (limits[WORKER_STAT_STAGE_APPLY] < 1) {
			limits[WORKER_STAT_STAGE_APPLY] = 1;
		}
		
		limits[WORKER_STAT_STAGE_CONVERT] = 1;
		limits[WORKER_STAT_STAGE_FLUSH] = 1;
	} else {
		idle = cache->num_workers - total;
		best_stage = WORKER_STAT_STAGE_APPLY;
		best_ratio = 0.0;

		for (int s = 0; s < WORKER_STAT_STAGE_NUM; s++) {
			if (speed[s] > 0.0) {
				ratio = demand[s] / speed[s];
			} else {
				ratio = demand[s];
			}

			if (ratio > best_ratio) {
				best_ratio = ratio;
				best_stage = s;
			}
		}

		limits[best_stage] += idle;
	}
}

/**
 * @brief   Derive worker start indices for each pipeline stage.
 *
 * When worker count is insufficient, adjusts @limits accordingly and places
 * CONVERT and FLUSH at the last worker.
 *
 * @param   cache:  Global cache instance.
 * @param   limits: Per-stage worker limits.
 * @param   start:  Output array sized WORKER_STAT_STAGE_NUM.
 */
void compute_stage_starts(struct trcache *cache, int *limits, int *start)
{
	if (cache->num_workers > WORKER_STAT_STAGE_NUM) {
		start[WORKER_STAT_STAGE_APPLY] = 0;
		start[WORKER_STAT_STAGE_CONVERT] =
			start[WORKER_STAT_STAGE_APPLY] +
				limits[WORKER_STAT_STAGE_APPLY];
		start[WORKER_STAT_STAGE_FLUSH] =
			start[WORKER_STAT_STAGE_CONVERT] +
				limits[WORKER_STAT_STAGE_CONVERT];
	} else {
		limits[WORKER_STAT_STAGE_APPLY] = cache->num_workers;
		limits[WORKER_STAT_STAGE_CONVERT] = 1;
		limits[WORKER_STAT_STAGE_FLUSH] = 1;
		start[WORKER_STAT_STAGE_APPLY] = 0;
		start[WORKER_STAT_STAGE_CONVERT] = cache->num_workers - 1;
		start[WORKER_STAT_STAGE_FLUSH] = cache->num_workers - 1;
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
