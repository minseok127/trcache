/**
 * @file   concurrent/atomsnap.c
 * @brief  Implementation of the atomic snapshot mechanism.
 *
 * This file implements a grace-period mechanism for managing a pointer.
 * The design packs an outer reference count and a version pointer into
 * a single 64-bit control block stored in the atomsnap_gate structure,
 * while the version itself (atomsnap_version) maintains an inner 
 * reference count.
 *
 * The 8-byte control block in atomsnap_gate is structured as follows:
 *   - Upper 16 bits: outer reference counter.
 *   - Lower 48 bits: pointer of the current version.
 *
 * Writers have their own version and each version can be concurrently read by
 * multiple readers. If a writer simply deallocates an old version to
 * replace it, readers might access wrong memory. To avoid this, multiple
 * versions are maintained.
 * 
 * When a reader wants to access the current version, it atomically increments
 * the outer reference counter using fetch_add(). The returned 64-bit value has
 * its lower 48-bits representing the pointer of the version whose reference
 * count was increased.
 *
 * After finishing the use of the version, the reader must release it. During release,
 * the reader increments the inner reference counter by 1. If the resulting inner
 * counter is 0, it indicates that no other threads are referencing that
 * version, so it can be freed.
 *
 * In the version replacement process, the writer atomically exchanges the 8-byte
 * control block with a new one (using atomic instruction), and the old control block,
 * which contains the previous version's outer reference count and version pointer, 
 * is returned. Because this update is atomic, new readers cannot access the old
 * version anymore. The writer then decrements the old version's inner counter by the
 * old outer reference count.
 *
 * Consequently, if a reader's release operation makes the inner counter to reach 0,
 * this reader is the last user of that version. If the writer's release operation
 * makes the inner counter to reach 0, this writer is the last user of that version.
 * Then the last user (reader or writer) can free the old version.
 *
 * cf) Why we divide the reference counter into two?
 * Because it is only possible to increment the reference counter in an 8-byte 
 * control block, but it is not possible to decrement. The reader wants to decrease
 * the reference count at the end. But the writer may changed the control block 
 * to the other version, so the reader can't use it. So, the reader must notice 
 * to the other reference counter, inner counter.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>

#include <assert.h>

#include "concurrent/atomsnap.h"
#include "utils/log.h"

#define OUTER_REF_CNT	(0x0001000000000000ULL)
#define OUTER_REF_MASK	(0xffff000000000000ULL)
#define OUTER_PTR_MASK	(0x0000ffffffffffffULL)
#define OUTER_REF_SHIFT	(48)

#define GET_OUTER_REFCNT(outer) ((outer & OUTER_REF_MASK) >> OUTER_REF_SHIFT)
#define GET_OUTER_PTR(outer)	(outer & OUTER_PTR_MASK)

/* Difference between outer counter and inner counter must be <= 0xffff */
#define WRAPAROUND_FACTOR (0x10000ULL)
#define WRAPAROUND_MASK    (0xffffULL)

/*
 * atomsnap_gate - Gate for atomic version read/write
 *
 * @atomsnap_alloc_impl:      User-defined memory allocation function.
 * @atomsnap_free_impl:       User-defined memory free function.
 * @control_block:            Control block to manage multi-versions.
 * @extra_control_blocks:     Array of extra control blocks.
 * @num_extra_control_blocks: Number of extra control blocks.
 *
 * Writers use atomsnap_gate to atomically register their object version.
 * Readers also use this gate to get the object and release safely.
 */
struct atomsnap_gate {
	struct atomsnap_version *(*atomsnap_alloc_impl)(void *alloc_arg);
	void (*atomsnap_free_impl)(struct atomsnap_version *version);
	_Atomic uint64_t control_block;
	_Atomic uint64_t *extra_control_blocks;
	int num_extra_control_blocks;
};

/**
 * @brief   Create a new atomsnap_gate.
 *
 * @param   ctx: Initialization context.
 *
 * @return  Pointer to atomsnap_gate on success, NULL on failure.
 */
struct atomsnap_gate *atomsnap_init_gate(struct atomsnap_init_context *ctx)
{
	atomsnap_gate *gate = calloc(1, sizeof(atomsnap_gate));

	if (gate == NULL) {
		errmsg(stderr, "Gate allocation failed\n");
		return NULL;
	}

	gate->atomsnap_alloc_impl = ctx->atomsnap_alloc_impl;
	gate->atomsnap_free_impl = ctx->atomsnap_free_impl;

	if (gate->atomsnap_alloc_impl == NULL || gate->atomsnap_free_impl == NULL) {
		errmsg(stderr, "Invalid alloc/free functions\n");
		free(gate);
		return NULL;
	}

	gate->num_extra_control_blocks = ctx->num_extra_control_blocks;

	if (gate->num_extra_control_blocks < 0) {
		errmsg(stderr, "Invalid num_extra_control_blocks\n");
		free(gate);
		return NULL;
	}

	if (gate->num_extra_control_blocks > 0) {
		gate->extra_control_blocks = calloc(gate->num_extra_control_blocks,
			sizeof(_Atomic uint64_t));

		if (gate->extra_control_blocks == NULL) {
			errmsg(stderr, "Failure on gate->extra_control_blocks\n");
			free(gate);
			return NULL;
		}
	}

	return gate;
}

/**
 * @brief   Destroy the given atomsnap_gate and all associated versions.
 *
 * @param   gate: Pointer returned by atomsnap_init_gate().
 */
void atomsnap_destroy_gate(struct atomsnap_gate *gate)
{
	if (gate == NULL) {
		return;
	}

	atomsnap_exchange_version_slot(gate, 0, NULL);

	if (gate->extra_control_blocks != NULL) {
		for (int i = 1; i <= gate->num_extra_control_blocks; i++) {
			atomsnap_exchange_version_slot(gate, i, NULL);
		}

		free(gate->extra_control_blocks);
	}

	free(gate);
}

/**
 * @brief  Allocate a new atomsnap_version using the allocation callback.
 *
 * @param  gate:              Target gate.
 * @param  alloc_version_arg: Argument forwarded to the allocation callback.
 *
 * @return  Pointer to the newly allocated version, or NULL on failure.
 *
 * The version's gate and opaque fields are initialized; the caller should
 * populate object and free_context as appropriate.
 */
struct atomsnap_version *atomsnap_make_version(struct atomsnap_gate *gate,
        void *alloc_arg)
{
	struct atomsnap_version *new_version = gate->atomsnap_alloc_impl(alloc_arg);

	if (new_version != NULL) {
		atomic_store(&new_version->gate, gate);
		atomic_store((int64_t *)(&new_version->opaque), 0);
	}

	return new_version;
}

static inline _Atomic uint64_t *atomsnap__slot(
	struct atomsnap_gate *gate, int idx)
{
	return idx ? &gate->extra_control_blocks[idx - 1] : &gate->control_block;
}

/**
 * @brief   Atomically acquire the current version from a slot.
 *
 * @param   gate:     Pointer to the gate.
 * @param   slot_idx: Zero-based slot index (0 for the default control block).
 *
 * @return  Pointer to the acquired version.
 */
struct atomsnap_version *atomsnap_acquire_version_slot(
        struct atomsnap_gate *gate, int slot_idx)
{
	_Atomic uint64_t *cb = atomsnap__slot(gate, slot_idx);
	uint64_t outer = atomic_fetch_add(cb, OUTER_REF_CNT);
	return (struct atomsnap_version *)GET_OUTER_PTR(outer);
}

/**
 * @brief   Release a version acquired via atomsnap_acquire_version_slot().
 *
 * The inner reference counter is decremented and the user-defined free
 * function is called when it reaches zero. Memory management is delegated
 * entirely to that callback.
 *
 * @param   version: Pointer to the version being released.
 */
void atomsnap_release_version(struct atomsnap_version *version)
{
	struct atomsnap_gate *gate = version->gate;
	int64_t inner_refcnt 
		= atomic_fetch_add((int64_t *)(&version->opaque), 1) + 1;

	if (inner_refcnt == 0) {
		gate->atomsnap_free_impl(version);
	}
}

/**
 * @brief   Unconditionally replace the version in a slot with @new_version.
 *
 * @param   gate:        Target gate.
 * @param   slot_idx:    Control block slot index.
 * @param   new_version: New version to register.
 *
 * Atomically swaps the control block to point to the new version. It then
 * decrements the inner reference count of the old version by the value of its
 * outer reference count at the time of the swap.
 *
 * The logic handles a potential wraparound of the 16-bit outer reference
 * counter. If the inner counter is still positive after the initial
 * decrement, it means the outer counter has wrapped around. In this case,
 * we subtract the wraparound factor (2^16) to correct the inner count.
 * The final inner count should be <= 0. If it is exactly 0, this writer
 * is the last user, and it can safely free the old version.
 */
void atomsnap_exchange_version_slot(struct atomsnap_gate *gate, int slot_idx,
        struct atomsnap_version *new_version)
{
	uint64_t old_outer, old_outer_refcnt;
	struct atomsnap_version *old_version;
	int64_t inner_refcnt;
	_Atomic uint64_t *cb;

	cb = atomsnap__slot(gate, slot_idx);
	old_outer = atomic_exchange(cb, (uint64_t)new_version);
	old_version = (struct atomsnap_version *)GET_OUTER_PTR(old_outer);

	if (old_version == NULL) {
		return;
	}

	/* Consider wraparound */
	atomic_fetch_and((int64_t *)(&old_version->opaque), WRAPAROUND_MASK);

	/* Decrease inner ref counter, we expect the result is minus */
	old_outer_refcnt = GET_OUTER_REFCNT(old_outer);
	inner_refcnt = atomic_fetch_sub((int64_t *)(&old_version->opaque),
		old_outer_refcnt) - old_outer_refcnt;

	/* The outer counter has been wraparound, adjust inner count */
	if (inner_refcnt > 0) {
		inner_refcnt = atomic_fetch_sub((int64_t *)(&old_version->opaque),
			WRAPAROUND_FACTOR) - WRAPAROUND_FACTOR;
	}
	assert(inner_refcnt <= 0);

	if (inner_refcnt == 0) {
		gate->atomsnap_free_impl(old_version);
	}
}

/**
 * @brief   Conditionally replace the version if @old_version matches.
 *
 * @param   gate:        Target gate.
 * @param   slot_idx:    Control block slot index.
 * @param   old_version: Expected current version.
 * @param   new_version: New version to register.
 *
 * @return  true on success, false otherwise.
 */
bool atomsnap_compare_exchange_version_slot(struct atomsnap_gate *gate,
        int slot_idx, struct atomsnap_version *old_version,
        struct atomsnap_version *new_version)
{
	uint64_t old_outer, old_outer_refcnt;
	int64_t inner_refcnt;
	_Atomic uint64_t *cb;

	cb = atomsnap__slot(gate, slot_idx);
	old_outer = atomic_load(cb);

	if (old_version != (struct atomsnap_version *)GET_OUTER_PTR(old_outer)
			|| !atomic_compare_exchange_weak(cb,
					&old_outer, (uint64_t)new_version)) {
		return false;
	} 

	if (old_version == NULL) {
		return true;
	}

	/* Consider wraparound */
	atomic_fetch_and((int64_t *)(&old_version->opaque), WRAPAROUND_MASK);

	/* Decrease inner ref counter, we expect the result minus */
	old_outer_refcnt = GET_OUTER_REFCNT(old_outer);
	inner_refcnt = atomic_fetch_sub((int64_t *)(&old_version->opaque),
		old_outer_refcnt) - old_outer_refcnt;

	/* The outer counter has been wraparound, adjust inner count */
	if (inner_refcnt > 0) {
		inner_refcnt = atomic_fetch_sub((int64_t *)(&old_version->opaque),
			WRAPAROUND_FACTOR) - WRAPAROUND_FACTOR;
	}
	assert(inner_refcnt <= 0);

	if (inner_refcnt == 0) {
		gate->atomsnap_free_impl(old_version);
	}

	return true;
}
