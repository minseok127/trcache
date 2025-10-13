#ifndef WORKER_STAT_BOARD_H
#define WORKER_STAT_BOARD_H

#include <stdint.h>
#include <string.h>

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
	struct worker_stage_stat apply_stat[MAX_CANDLE_TYPES];
	struct worker_stage_stat convert_stat[MAX_CANDLE_TYPES];
	struct worker_stage_stat flush_stat[MAX_CANDLE_TYPES];
};

/**
 * @brief   Add @cycles and @count to APPLY stage statistics.
 */
static inline void worker_stat_add_apply(struct worker_stat_board *board,
	int candle_idx, uint64_t cycles, uint64_t count)
{
	board->apply_stat[candle_idx].cycles += cycles;
	board->apply_stat[candle_idx].work_count += count;
}

/**
 * @brief   Add @cycles and @count to CONVERT stage statistics.
 */
static inline void worker_stat_add_convert(struct worker_stat_board *board,
	int candle_idx, uint64_t cycles, uint64_t count)
{
	board->convert_stat[candle_idx].cycles += cycles;
	board->convert_stat[candle_idx].work_count += count;
}

/**
 * @brief   Add @cycles and @count to FLUSH stage statistics.
 */
static inline void worker_stat_add_flush(struct worker_stat_board *board,
	int candle_idx, uint64_t cycles, uint64_t count)
{
	board->flush_stat[candle_idx].cycles += cycles;
	board->flush_stat[candle_idx].work_count += count;
}

/**
 * @brief   Reset all counters in the board to zero.
 */
static inline void worker_stat_reset(struct worker_stat_board *board)
{
	memset(board->apply_stat, 0,
		sizeof(struct worker_stage_stat) * MAX_CANDLE_TYPES);
	memset(board->convert_stat, 0,
		sizeof(struct worker_stage_stat) * MAX_CANDLE_TYPES);
	memset(board->flush_stat, 0,
		sizeof(struct worker_stage_stat) * MAX_CANDLE_TYPES);
}

#endif /* WORKER_STAT_BOARD_H */
