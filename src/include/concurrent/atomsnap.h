#ifndef ATOMSNAP_H
#define ATOMSNAP_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

/*
 * atomsnap_gate - Gate for atomic version read/write
 *
 * Readers and writers using the same gate access different versions of an
 * object concurrently.
 */
typedef struct atomsnap_gate atomsnap_gate;

/*
 * atomsnap_version - Target object's version
 *
 * Writers obtain a pointer to atomsnap_version through atomsnap_make_version()
 * and populate @object and @free_context as needed.  The @gate and @opaque
 * fields are managed internally.
 *
 * @object:       Pointer to the actual data of this version.
 * @free_context: Context used by the user-defined free function.
 * @gate:         Gate this version belongs to.
 * @opaque:       Internal data for version management.
 */
typedef struct atomsnap_version {
	void *object;
	void *free_context;
	struct atomsnap_gate *gate;
	void *opaque;
} atomsnap_version;

/*
 * atomsnap_init_context - Parameters for atomsnap_init_gate()
 *
 * Users supply custom allocation and free callbacks and optionally request
 * extra control blocks.
 *
 * @atomsnap_alloc_impl:  User-defined version allocation function.
 * @atomsnap_free_impl:   User-defined version free function.
 * @num_extra_control_blocks: Number of extra control blocks to create.
 *
 * If @num_extra_control_blocks is zero, a single control block is used. When
 * greater than zero, slots 1..N are provided in addition to the default slot 0.
 */
typedef struct atomsnap_init_context {
	struct atomsnap_version *(*atomsnap_alloc_impl)(void* alloc_arg);
	void (*atomsnap_free_impl)(struct atomsnap_version *version);
	int num_extra_control_blocks;
} atomsnap_init_context;

/**
 * @brief   Create a new atomsnap_gate instance.
 *
 * @param   ctx: Initialization context.
 *
 * @return  Pointer to an atomsnap_gate on success, NULL on failure.
 */
struct atomsnap_gate *atomsnap_init_gate(struct atomsnap_init_context *ctx);

/**
 * @brief   Destroy the given atomsnap_gate.
 *
 * @param   gate: Gate returned by atomsnap_init_gate().
 */
void atomsnap_destroy_gate(struct atomsnap_gate *gate);

/**
 * @brief   Allocate a new atomsnap_version via the user-defined callback.
 *
 * The returned version has its gate and internal opaque data initialized. The
 * caller is responsible for populating the object and free_context fields.
 *
 * @param   gate:       Target gate returned by atomsnap_init_gate().
 * @param   alloc_arg:  Argument forwarded to the allocation callback.
 *
 * @return  Pointer to a new atomsnap_version on success, NULL on failure.
 */
struct atomsnap_version *atomsnap_make_version(struct atomsnap_gate *gate,
        void *alloc_arg);

/**
 * @brief   Atomically acquire the current version from a slot.
 *
 * @param   gate:     Target gate.
 * @param   slot_idx: Control block slot index (0 for the default slot).
 *
 * @return  Pointer to the acquired version.
 */
struct atomsnap_version *atomsnap_acquire_version_slot(
        struct atomsnap_gate *gate, int slot_idx);

/**
 * @brief   Release a version previously acquired with atomsnap_acquire_version().
 *
 * If the reference count reaches zero, the user-defined free callback is
 * invoked. Memory for the version and its object is managed by that callback.
 *
 * @param   version: Version to release.
 */
void atomsnap_release_version(struct atomsnap_version *version);

/**
 * @brief   Replace the version in the given slot unconditionally.
 *
 * @param   gate:       Target gate.
 * @param   slot_idx:   Control block slot index.
 * @param   new_ver:    New version to register.
 */
void atomsnap_exchange_version_slot(struct atomsnap_gate *gate, int slot_idx,
        struct atomsnap_version *new_ver);

/**
 * @brief   Conditionally replace the version if @old_ver matches.
 *
 * @param   gate:       Target gate.
 * @param   slot_idx:   Control block slot index.
 * @param   old_ver:    Expected current version.
 * @param   new_ver:    New version to register.
 *
 * @return  true on successful exchange, false otherwise.
 */
bool atomsnap_compare_exchange_version_slot(struct atomsnap_gate *gate,
        int slot_idx, struct atomsnap_version *old_ver,
        struct atomsnap_version *new_ver);

/*
 * Convenience wrappers that preserve perfect backward compatibility:
 * they operate on slot 0, i.e., the original single control block.
 */
#define atomsnap_acquire_version(g) \
	atomsnap_acquire_version_slot((g), 0)

#define atomsnap_exchange_version(g, v) \
	atomsnap_exchange_version_slot((g), 0, (v))

#define atomsnap_compare_exchange_version(g, o, n) \
	atomsnap_compare_exchange_version_slot((g), 0, (o), (n))

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* ATOMSNAP_H */
