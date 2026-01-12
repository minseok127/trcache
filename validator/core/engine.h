/*
 * validator/core/engine.h
 *
 * Wrapper module for the trcache engine.
 * Handles initialization, configuration mapping, and lifecycle management.
 * Bridges the gap between C++ runtime configuration and C-style callbacks.
 */

#ifndef VALIDATOR_CORE_ENGINE_H
#define VALIDATOR_CORE_ENGINE_H

#include "trcache.h"
#include "config.h"

/*
 * engine_init - Initialize the trcache engine.
 *
 * Converts the validator configuration into a trcache context,
 * sets up the appropriate callback functions (tick/time), and
 * allocates the engine.
 *
 * @config: The global configuration object.
 * @return: Pointer to the initialized trcache instance, or nullptr on failure.
 */
struct trcache* engine_init(const struct validator_config& config);

/*
 * engine_destroy - Shutdown and free the engine.
 *
 * @cache: Pointer to the trcache instance to destroy.
 */
void engine_destroy(struct trcache* cache);

#endif /* VALIDATOR_CORE_ENGINE_H */
