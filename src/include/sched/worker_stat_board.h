#ifndef WORKER_STAT_BOARD_H
#define WORKER_STAT_BOARD_H

#include <stdint.h>

#include "trcache.h"

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
	struct worker_stage_stat apply_stat[NUM_CANDLE_BASES][MAX_CANDLE_TYPES_PER_BASE];
	struct worker_stage_stat convert_stat[NUM_CANDLE_BASES][MAX_CANDLE_TYPES_PER_BASE];
	struct worker_stage_stat flush_stat[NUM_CANDLE_BASES][MAX_CANDLE_TYPES_PER_BASE];
};

/**
 * @brief   Add @cycles and @count to APPLY stage statistics.
 */
static inline void worker_stat_add_apply(struct worker_stat_board *board,
       trcache_candle_type type, uint64_t cycles, uint64_t count)
{
	board->apply_stat[type.base_type][type.type_idx].cycles += cycles;
	board->apply_stat[type.base_type][type.type_idx].work_count += count;
}

/**
 * @brief   Add @cycles and @count to CONVERT stage statistics.
 */
static inline void worker_stat_add_convert(struct worker_stat_board *board,
       trcache_candle_type type, uint64_t cycles, uint64_t count)
{
	board->convert_stat[type.base_type][type.type_idx].cycles += cycles;
	board->convert_stat[type.base_type][type.type_idx].work_count += count;
}

/**
 * @brief   Add @cycles and @count to FLUSH stage statistics.
 */
static inline void worker_stat_add_flush(struct worker_stat_board *board,
       trcache_candle_type type, uint64_t cycles, uint64_t count)
{
	board->flush_stat[type.base_type][type.type_idx].cycles += cycles;
	board->flush_stat[type.base_type][type.type_idx].work_count += count;
}

/**
 * @brief   Reset all counters in the board to zero.
 */
static inline void worker_stat_reset(struct worker_stat_board *board)
{
	for (int i = 0; i < NUM_CANDLE_BASES; ++i) {
		for (int j = 0; j < MAX_CANDLE_TYPES_PER_BASE; ++j) {
			board->apply_stat[i][j].cycles = 0;
			board->apply_stat[i][j].work_count = 0;
			board->convert_stat[i][j].cycles = 0;
			board->convert_stat[i][j].work_count = 0;
			board->flush_stat[i][j].cycles = 0;
			board->flush_stat[i][j].work_count = 0;
		}
	}
}

#endif /* WORKER_STAT_BOARD_H */
