/*
 * validator/core/config.h
 *
 * Defines the configuration structures for the validator application.
 * These structures map directly to the JSON configuration file and carry
 * runtime settings for the engine, target symbols, and network connections.
 */

#ifndef VALIDATOR_CORE_CONFIG_H
#define VALIDATOR_CORE_CONFIG_H

#include <string>
#include <vector>
#include <map>

/*
 * val_candle_config - Configuration for a single candle type.
 *
 * Defines the logic and threshold for candle generation.
 * e.g., type="TICK_MODULO", threshold=10
 */
struct val_candle_config {
	std::string name;
	std::string type;	/* Logic type: "TICK_MODULO", "TIME_FIXED" */
	int threshold;		/* Value: 10 (ticks) or 60000 (ms) */
};

/*
 * validator_config - Global configuration object.
 *
 * Holds all settings loaded from config.json. Passed to the exchange
 * adapter during initialization.
 */
struct validator_config {
	/* Exchange Selection (e.g., "binance", "upbit") */
	std::string exchange;

	/* Target Selection */
	int top_n;				/* 0 if using manual symbols */
	std::vector<std::string> manual_symbols;

	/* Engine Settings */
	size_t memory_limit_mb;
	int worker_threads;
	int batch_size_pow2;
	int cached_batch_count_pow2;

	/* Candle Specifications */
	std::vector<struct val_candle_config> candles;

	/* Network Settings */
	std::string ws_endpoint;
	std::string rest_endpoint;

	/*
	 * Exchange-Specific Options.
	 * Stores arbitrary key-value pairs (e.g., API keys, secrets)
	 * that vary by exchange.
	 */
	std::map<std::string, std::string> options;
};

#endif /* VALIDATOR_CORE_CONFIG_H */
