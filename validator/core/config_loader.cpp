/*
 * validator/core/config_loader.cpp
 *
 * Implementation of the configuration loader using nlohmann/json.
 * Maps JSON fields to the C++ validator_config structure.
 */

#include "config_loader.h"

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

/* Use standard namespace for readability in implementation */
using json = nlohmann::json;

/*
 * load_config - Implementation
 */
bool load_config(const std::string& path, struct validator_config* config)
{
	std::ifstream file(path);
	if (!file.is_open()) {
		std::cerr << "[Config] Error: Cannot open config file: "
			  << path << std::endl;
		return false;
	}

	try {
		json j;
		file >> j;

		/* 1. Global Exchange Setting */
		config->exchange = j.value("exchange", "binance");

		/* 2. Target Settings */
		if (j.contains("target")) {
			auto& t = j["target"];
			config->top_n = t.value("top_n", 0);
			
			if (t.contains("manual_symbols")) {
				config->manual_symbols =
					t["manual_symbols"]
					.get<std::vector<std::string>>();
			}
		} else {
			config->top_n = 5;
		}

		/* 3. Engine Settings */
		if (j.contains("engine")) {
			auto& e = j["engine"];
			config->memory_limit_mb =
				e.value("memory_limit_mb", 1024);
			config->worker_threads =
				e.value("worker_threads", 4);
			config->batch_size_pow2 =
				e.value("batch_size_pow2", 10);
			config->cached_batch_count_pow2 =
				e.value("cached_batch_count_pow2", 4);
		} else {
			config->memory_limit_mb = 1024;
			config->worker_threads = 4;
			config->batch_size_pow2 = 10;
			config->cached_batch_count_pow2 = 4;
		}

		/* 4. Candle Specifications */
		if (j.contains("candles") && j["candles"].is_array()) {
			for (const auto& item : j["candles"]) {
				struct val_candle_config c;
				c.name = item.value("name", "unknown");
				c.type = item.value("type", "TIME_FIXED");
				c.threshold = item.value("threshold", 60000);
				
				config->candles.push_back(c);
			}
		}

		/* 5. Network Settings (Standardized) */
		if (j.contains("network")) {
			auto& n = j["network"];
			config->ws_endpoint = n.value("ws_url", "");
			config->rest_endpoint = n.value("rest_url", "");
		}
		
		/*
		 * Fallback for legacy/specific config (e.g. binance defaults)
		 * if network block is missing but exchange is binance.
		 */
		if (config->ws_endpoint.empty() && 
		    config->exchange == "binance") {
			config->ws_endpoint =
				"wss://stream.binance.com:9443/ws";
			config->rest_endpoint =
				"https://api.binance.com";
		}

		/* 6. Exchange-Specific Options (Generic Map) */
		if (j.contains("options") && j["options"].is_object()) {
			for (auto& el : j["options"].items()) {
				/* Store as string-string pairs */
				if (el.value().is_string()) {
					config->options[el.key()] =
						el.value();
				} else {
					/* Convert numbers/bools to string */
					config->options[el.key()] =
						el.value().dump();
				}
			}
		}

	} catch (const json::exception& e) {
		std::cerr << "[Config] JSON Parse Error: " << e.what()
			  << std::endl;
		return false;
	}

	return true;
}
