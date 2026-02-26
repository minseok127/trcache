#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include <stdatomic.h>
#include <stdint.h>

#include "trcache.h"

/*
 * Bitmap layout macros
 * ====================
 * The admin thread packs task assignments into three flat bitmaps per
 * worker. Workers decode each set bit back to (type_idx, sym_idx).
 *
 * All candle tasks share the same linear task index:
 *   task_idx = type_idx * capacity + sym_idx
 *
 * in_memory_bitmap — 2 bits per task:
 *   bit [task_idx * 2]     → Apply   task for (type_idx, sym_idx)
 *   bit [task_idx * 2 + 1] → Convert task for (type_idx, sym_idx)
 *   Each task occupies a 2-bit slot so the LSB selects the stage.
 *
 * batch_flush_bitmap — 1 bit per task:
 *   bit [task_idx] → Batch Flush task for (type_idx, sym_idx)
 *   One bit suffices because only one Batch Flush stage exists.
 *
 * trade_flush_bitmap — 1 bit per symbol:
 *   bit [sym_idx] → Trade Flush task for sym_idx
 *   Independent of candle type; one bit per symbol.
 */

/*
 * Candle task linearisation.
 * Tasks are laid out row-major (type outer, sym inner):
 *   task_idx = type_idx * capacity + sym_idx
 * Shared by both in_memory_bitmap and batch_flush_bitmap.
 */
#define CANDLE_TASK_IDX(type, sym, cap)  ((type) * (cap) + (sym))

/*
 * in_memory_bitmap — encode.
 * Multiply task_idx by 2 to open a 2-bit slot per task.
 * Slot[0] (even bit) = Apply, Slot[1] (odd bit) = Convert.
 */
#define IM_APPLY_BIT(task_idx)           ((task_idx) * 2)
#define IM_CONVERT_BIT(task_idx)         ((task_idx) * 2 + 1)

/*
 * in_memory_bitmap — decode.
 * Divide by 2 to collapse the 2-bit slot back to task_idx.
 * Even bit position → Apply; odd bit position → Convert.
 */
#define IM_BIT_TO_TASK_IDX(bit)          ((bit) / 2)
#define IM_BIT_IS_APPLY(bit)             (((bit) % 2) == 0)

/*
 * Shared decode: recover (type_idx, sym_idx) from a linear task_idx.
 * Inverse of CANDLE_TASK_IDX: integer division gives type, modulo gives sym.
 */
#define TASK_TO_TYPE_IDX(task, cap)      ((task) / (cap))
#define TASK_TO_SYM_IDX(task, cap)       ((task) % (cap))

/*
 * trade_flush_bitmap — encode/decode.
 * Bit position equals sym_idx directly; no transformation needed.
 */
#define TF_BIT(sym_idx)                  (sym_idx)
#define TF_BIT_TO_SYM_IDX(bit)          (bit)

/**
 * @brief Defines the operational group a worker belongs to.
 *
 * The admin thread sets this identifier to inform the worker which
 * bitmap (in-memory or flush) it should scan for work.
 */
enum worker_group_type {
	GROUP_IN_MEMORY,
	GROUP_BATCH_FLUSH
};

/**
 * worker_state - Per-worker runtime data.
 *
 * @worker_id:           Numeric ID assigned to the worker thread.
 * @done:                Flag signalled during shutdown.
 * @group_id:            Identifier for the worker's current assigned group.
 * @in_memory_bitmap:    Pointer to the cacheline-aligned bitmap for
 *                       Apply/Convert tasks.
 * @batch_flush_bitmap:  Pointer to the cacheline-aligned bitmap for
 *                       candle batch Flush tasks.
 * @trade_flush_bitmap:  Pointer to the cacheline-aligned bitmap for
 *                       raw trade chunk Flush tasks.
 */
struct worker_state {
	int worker_id;
	_Atomic(bool) done;
	_Atomic(int) group_id;
	uint64_t *in_memory_bitmap;
	uint64_t *batch_flush_bitmap;
	uint64_t *trade_flush_bitmap;
};

/**
 * @brief   Initialise the worker thread state.
 *
 * @param   tc:        Owner of the admin state.
 * @param   worker_id: Numeric identifier for the worker.
 *
 * @return  0 on success, -1 on failure.
 */
int worker_state_init(struct trcache *tc, int worker_id);

/**
 * @brief   Destroy resources held by @state.
 *
 * @param   state:   Previously initialised worker_state pointer.
 */
void worker_state_destroy(struct worker_state *state);

/*
 * Arguments of the worker_thread_main().
 */
struct worker_thread_args {
	struct trcache *cache;
	int worker_id;
};

/**
 * @brief   Entry point for a worker thread.
 *
 * Expects a ::worker_thread_args pointer as its argument.
 *
 * @param   arg: Opaque pointer cast from ::worker_thread_args.
 *
 * @return  Always returns NULL.
 */
void *worker_thread_main(void *arg);

#endif /* WORKER_THREAD_H */
