#ifndef WORKER_STAT_BOARD_H
#define WORKER_STAT_BOARD_H

#include <stdint.h>

#include "trcache.h"

/**
 * Identifiers for pipeline stages executed by workers.
 */
typedef enum worker_stat_stage_type {
	WORKER_STAT_STAGE_APPLY = 0,
	WORKER_STAT_STAGE_CONVERT,
	WORKER_STAT_STAGE_FLUSH,
	WORKER_STAT_STAGE_NUM
} worker_stat_stage_type;

/**
 * worker_stage_stat - Accumulated performance counters for one stage.
 *
 * @cycles:     Total cycle count spent on this stage.
 * @work_count: Number of processed work units.
 */
struct worker_stage_stat {
	uint64_t cycles;
	uint64_t work_count;
};

/**
 * worker_stat_board - Per-worker performance table split per stage.
 */
struct worker_stat_board {
	struct worker_stage_stat apply_stat[TRCACHE_NUM_CANDLE_TYPE];
	struct worker_stage_stat convert_stat[TRCACHE_NUM_CANDLE_TYPE];
	struct worker_stage_stat flush_stat[TRCACHE_NUM_CANDLE_TYPE];
};

/**
 * @brief Translate candle type bit to a zero-based index.
 */
static inline int worker_ct_to_idx(trcache_candle_type type)
{
	return __builtin_ctz(type);
}

/**
 * @brief   Add @cycles and @count to APPLY stage statistics.
 */
static inline void worker_stat_add_apply(struct worker_stat_board *board,
       trcache_candle_type type, uint64_t cycles, uint64_t count)
{
       int idx = worker_ct_to_idx(type);

       board->apply_stat[idx].cycles += cycles;
       board->apply_stat[idx].work_count += count;
}

/**
 * @brief   Add @cycles and @count to CONVERT stage statistics.
 */
static inline void worker_stat_add_convert(struct worker_stat_board *board,
       trcache_candle_type type, uint64_t cycles, uint64_t count)
{
       int idx = worker_ct_to_idx(type);

       board->convert_stat[idx].cycles += cycles;
       board->convert_stat[idx].work_count += count;
}

/**
 * @brief   Add @cycles and @count to FLUSH stage statistics.
 */
static inline void worker_stat_add_flush(struct worker_stat_board *board,
       trcache_candle_type type, uint64_t cycles, uint64_t count)
{
       int idx = worker_ct_to_idx(type);

       board->flush_stat[idx].cycles += cycles;
       board->flush_stat[idx].work_count += count;
}

/**
 * @brief   Reset all counters in the board to zero.
 */
static inline void worker_stat_reset(struct worker_stat_board *board)
{
	for (int t = 0; t < TRCACHE_NUM_CANDLE_TYPE; t++) {
		board->apply_stat[t].cycles = 0;
		board->apply_stat[t].work_count = 0;
		board->convert_stat[t].cycles = 0;
		board->convert_stat[t].work_count = 0;
		board->flush_stat[t].cycles = 0;
		board->flush_stat[t].work_count = 0;
	}
}

#endif /* WORKER_STAT_BOARD_H */
