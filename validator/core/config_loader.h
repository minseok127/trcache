/*
 * validator/core/config_loader.h
 *
 * Declares the configuration loading utility.
 * Responsible for reading the JSON file and populating the
 * validator_config structure.
 */

#ifndef VALIDATOR_CORE_CONFIG_LOADER_H
#define VALIDATOR_CORE_CONFIG_LOADER_H

#include <string>
#include "config.h"

/*
 * load_config - Load and parse the JSON configuration file.
 *
 * Reads the file at the specified path, parses it using nlohmann/json,
 * and populates the provided validator_config structure.
 *
 * @path:   Path to the JSON configuration file.
 * @config: Pointer to the configuration structure to populate.
 * @return: true if successful, false if file not found or parse error.
 */
bool load_config(const std::string& path, struct validator_config* config);

#endif /* VALIDATOR_CORE_CONFIG_LOADER_H */
