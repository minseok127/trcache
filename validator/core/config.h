/*
 * validator/core/config.h
 *
 * Configuration structures for the validator.
 */

#ifndef VALIDATOR_CORE_CONFIG_H
#define VALIDATOR_CORE_CONFIG_H

#include <string>
#include <vector>
#include <map> /* Added for options map */

/*
 * Candle configuration structure
 */
struct val_candle_config {
	std::string type;       /* "TICK_MODULO" or "TIME_FIXED" */
	int threshold;          /* Tick count or Time in ms */
	std::string name;       /* Display name for logging */
};

/*
 * Main validator configuration structure
 */
struct validator_config {
	/* Global Settings */
	std::string exchange;   /* "binance", etc. */

	/* Market Data Settings */
	std::string ws_endpoint;
	std::string rest_endpoint;
	int top_n;
	std::vector<std::string> manual_symbols;

	/* Engine Settings */
	int worker_threads;
	int memory_limit_mb;
	int batch_size_pow2;
	int cached_batch_count_pow2;

	/* Candle Settings */
	std::vector<val_candle_config> candles;

	/* Reporting Settings */
	std::string csv_output_path; /* Path to save CSV report */
	bool csv_append_timestamp;   /* Append timestamp to filename */

	/* Misc Options */
	std::map<std::string, std::string> options;
};

#endif /* VALIDATOR_CORE_CONFIG_H */
