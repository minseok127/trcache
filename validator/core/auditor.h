/*
 * validator/core/auditor.h
 *
 * Defines the Auditor module.
 * It runs in a separate thread to verify data integrity and latency.
 */

#ifndef VALIDATOR_CORE_AUDITOR_H
#define VALIDATOR_CORE_AUDITOR_H

#include <atomic>
#include "trcache.h"
#include "config.h"

/*
 * run_auditor - The main loop for the verification thread.
 *
 * @cache:        Pointer to the initialized trcache engine.
 * @config:       Configuration object containing symbol counts and settings.
 * @running_flag: Global atomic flag to control the loop termination.
 */
void run_auditor(struct trcache* cache,
		 const struct validator_config& config,
		 std::atomic<bool>& running_flag);

#endif /* VALIDATOR_CORE_AUDITOR_H */
