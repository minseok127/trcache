/*
 * validator/core/config_loader.cpp
 *
 * Implementation of the configuration loader using simdjson.
 * Maps JSON fields to the C++ validator_config structure.
 */

#include "config_loader.h"

#include <iostream>
#include <simdjson.h>

/*
 * load_config - Implementation using simdjson
 */
bool load_config(const std::string& path, struct validator_config* config)
{
	simdjson::dom::parser parser;
	simdjson::dom::element doc;
	
	/* Load and parse file directly */
	simdjson::error_code error = parser.load(path).get(doc);
	if (error) {
		std::cerr << "[Config] Error: Cannot parse config file: "
			  << path << " (" << error << ")" << std::endl;
		return false;
	}

	try {
		/* 1. Global Exchange Setting */
		std::string_view ex_sv;
		if (doc["exchange"].get(ex_sv) == simdjson::SUCCESS) {
			config->exchange = std::string(ex_sv);
		} else {
			config->exchange = "binance";
		}

		/* 2. Target Settings */
		simdjson::dom::element target;
		if (doc["target"].get(target) == simdjson::SUCCESS) {
			uint64_t top_n;
			if (target["top_n"].get(top_n) == simdjson::SUCCESS) {
				config->top_n = (int)top_n;
			} else {
				config->top_n = 5;
			}

			simdjson::dom::array manual_syms;
			if (target["manual_symbols"].get(manual_syms) ==
			    simdjson::SUCCESS) {
				for (std::string_view sym : manual_syms) {
					config->manual_symbols.emplace_back(sym);
				}
			}
		} else {
			config->top_n = 5;
		}

		/* 3. Engine Settings */
		simdjson::dom::element eng;
		if (doc["engine"].get(eng) == simdjson::SUCCESS) {
			uint64_t val;
			if (eng["memory_limit_mb"].get(val) == simdjson::SUCCESS)
				config->memory_limit_mb = (int)val;
			else 
				config->memory_limit_mb = 1024;

			if (eng["worker_threads"].get(val) == simdjson::SUCCESS)
				config->worker_threads = (int)val;
			else
				config->worker_threads = 4;

			if (eng["batch_size_pow2"].get(val) == simdjson::SUCCESS)
				config->batch_size_pow2 = (int)val;
			else
				config->batch_size_pow2 = 10;
			
			if (eng["cached_batch_count_pow2"].get(val) ==
			    simdjson::SUCCESS)
				config->cached_batch_count_pow2 = (int)val;
			else
				config->cached_batch_count_pow2 = 4;
		} else {
			config->memory_limit_mb = 1024;
			config->worker_threads = 4;
			config->batch_size_pow2 = 10;
			config->cached_batch_count_pow2 = 4;
		}

		/* 4. Candle Specifications */
		simdjson::dom::array candles;
		if (doc["candles"].get(candles) == simdjson::SUCCESS) {
			for (simdjson::dom::element item : candles) {
				struct val_candle_config c;
				std::string_view sv;
				uint64_t u64;

				if (item["name"].get(sv) == simdjson::SUCCESS)
					c.name = std::string(sv);
				else
					c.name = "unknown";

				if (item["type"].get(sv) == simdjson::SUCCESS)
					c.type = std::string(sv);
				else
					c.type = "TIME_FIXED";

				if (item["threshold"].get(u64) == simdjson::SUCCESS)
					c.threshold = (int)u64;
				else
					c.threshold = 60000;
				
				config->candles.push_back(c);
			}
		}

		/* 5. Network Settings */
		simdjson::dom::element net;
		if (doc["network"].get(net) == simdjson::SUCCESS) {
			std::string_view sv;
			if (net["ws_url"].get(sv) == simdjson::SUCCESS)
				config->ws_endpoint = std::string(sv);
			
			if (net["rest_url"].get(sv) == simdjson::SUCCESS)
				config->rest_endpoint = std::string(sv);
		}

		if (config->ws_endpoint.empty() && 
		    config->exchange == "binance") {
			config->ws_endpoint =
				"wss://stream.binance.com:9443/ws";
			config->rest_endpoint =
				"https://api.binance.com";
		}

		/* 6. Options */
		simdjson::dom::element opts;
		if (doc["options"].get(opts) == simdjson::SUCCESS &&
		    opts.is_object()) {
			simdjson::dom::object obj = opts;
			for (auto field : obj) {
				std::string key(field.key);
				std::string val_str;

				/* Handle type differences explicitly */
				switch (field.value.type()) {
				case simdjson::dom::element_type::STRING:
					val_str = std::string(
						field.value.get_string().value());
					break;
				case simdjson::dom::element_type::INT64:
					val_str = std::to_string(
						field.value.get_int64().value());
					break;
				case simdjson::dom::element_type::UINT64:
					val_str = std::to_string(
						field.value.get_uint64().value());
					break;
				case simdjson::dom::element_type::BOOL:
					val_str = field.value.get_bool().value() ?
						"true" : "false";
					break;
				default:
					continue;
				}
				config->options[key] = val_str;
			}
		}

	} catch (const simdjson::simdjson_error& e) {
		std::cerr << "[Config] Simdjson Access Error: " << e.what()
			  << std::endl;
		return false;
	}

	return true;
}
