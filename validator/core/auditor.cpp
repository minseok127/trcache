/*
 * validator/core/auditor.cpp
 *
 * Implementation of the Auditor module.
 *
 * This module acts as the quality assurance layer for the validator.
 * It performs two key verification tasks:
 * 1. Internal Integrity: Checks for sequence gaps in candle trades.
 * 2. System Latency: Measures Engine Latency and Total System Latency.
 */

#include "auditor.h"
#include "types.h"

#include <iostream>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <cstring>
#include <time.h>
#include <iomanip>

/*
 * --------------------------------------------------------------------------
 * Internal Structures
 * --------------------------------------------------------------------------
 */

struct symbol_cursor {
	uint64_t last_key;
	uint64_t last_end_seq_id;
	bool initialized;

	symbol_cursor() : last_key(0), last_end_seq_id(0), initialized(false) {}
};

struct latency_stats {
	std::vector<double> engine_samples;
	std::vector<double> total_samples;
	uint64_t gap_count;
	uint64_t total_candles;

	latency_stats() : gap_count(0), total_candles(0) {
		engine_samples.reserve(4096);
		total_samples.reserve(4096);
	}

	void add(double engine_lat, double total_lat) {
		engine_samples.push_back(engine_lat);
		total_samples.push_back(total_lat);
		total_candles++;
	}

	double get_avg(const std::vector<double>& v) {
		if (v.empty()) return 0;
		double sum = 0;
		for (double d : v) sum += d;
		return sum / v.size();
	}

	double get_p99(std::vector<double>& v) {
		if (v.empty()) return 0;
		std::sort(v.begin(), v.end());
		return v[v.size() * 0.99];
	}

	double get_max(std::vector<double>& v) {
		if (v.empty()) return 0;
		return v.back();
	}

	void report_and_reset() {
		if (total_candles == 0) return;

		double eng_avg = get_avg(engine_samples);
		double eng_p99 = get_p99(engine_samples);
		double eng_max = get_max(engine_samples);

		double tot_avg = get_avg(total_samples);
		double tot_p99 = get_p99(total_samples);
		double tot_max = get_max(total_samples);

		std::cout << "[Auditor] Processed: " << total_candles
			  << " | Gaps: " << gap_count
			  << std::fixed << std::setprecision(3)
			  << " | Eng Lat(ms) Avg: " << eng_avg / 1000.0
			  << " P99: " << eng_p99 / 1000.0
			  << " Max: " << eng_max / 1000.0
			  << " | Tot Lat(ms) Avg: " << tot_avg / 1000.0
			  << " P99: " << tot_p99 / 1000.0
			  << " Max: " << tot_max / 1000.0
			  << std::endl;

		engine_samples.clear();
		total_samples.clear();
		engine_samples.reserve(4096);
		total_samples.reserve(4096);
		total_candles = 0;
	}
};

/*
 * --------------------------------------------------------------------------
 * Helper Functions
 * --------------------------------------------------------------------------
 */

static inline uint64_t get_micros()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t)ts.tv_sec * 1000000ULL +
	       (uint64_t)ts.tv_nsec / 1000;
}

/*
 * --------------------------------------------------------------------------
 * Main Logic
 * --------------------------------------------------------------------------
 */

void run_auditor(struct trcache* cache,
		 const struct validator_config& config,
		 std::atomic<bool>& running_flag)
{
	std::cout << "[Auditor] Thread started. Allocating batch buffers..."
		  << std::endl;

	int num_configs = config.candles.size();
	int max_symbols = config.top_n + 50;
	int total_streams = max_symbols * num_configs;

	/* Prepare Field Request */
	int req_indices[] = {
		VAL_COL_EXCHANGE_TS,
		VAL_COL_LOCAL_TS,
		VAL_COL_START_SEQ_ID,
		VAL_COL_END_SEQ_ID
	};
	trcache_field_request req = {
		req_indices,
		sizeof(req_indices) / sizeof(int)
	};

	/* Allocate Batches (One per config type) */
	std::vector<trcache_candle_batch*> batches(num_configs);
	for (int i = 0; i < num_configs; ++i) {
		/* Allocate enough buffer to fetch multiple candles at once */
		batches[i] = trcache_batch_alloc_on_heap(cache, i, 32, &req);
		if (!batches[i]) {
			std::cerr << "[Auditor] Batch alloc failed for cfg "
				  << i << std::endl;
			return;
		}
	}

	std::vector<symbol_cursor> cursors(total_streams);
	latency_stats stats;

	auto last_report = std::chrono::steady_clock::now();

	while (running_flag) {
		bool busy = false;

		for (int sym_id = 0; sym_id < max_symbols; ++sym_id) {
			for (int cfg_idx = 0; cfg_idx < num_configs; ++cfg_idx) {
				
				int idx = (sym_id * num_configs) + cfg_idx;
				symbol_cursor& cur = cursors[idx];
				trcache_candle_batch* batch = batches[cfg_idx];
				
				/*
				 * 1. Check the latest candle (Offset 0) first.
				 * This avoids unnecessary large fetches if nothing changed.
				 */
				int ret = trcache_get_candles_by_symbol_id_and_offset(
						cache, sym_id, cfg_idx, &req, 0, 1, batch);
				
				if (ret != 0 || batch->num_candles == 0) {
					continue;
				}

				/*
				 * trcache batch order is typically Time-Ascending.
				 * So index 0 is the result of the query (Newest).
				 */
				uint64_t latest_key = batch->key_array[0];
				
				/* Initialization */
				if (!cur.initialized) {
					/* Wait for a closed candle to start tracking */
					if (batch->is_closed_array[0]) {
						uint64_t* end_ids = (uint64_t*)
							batch->column_arrays[VAL_COL_END_SEQ_ID];
						cur.last_key = latest_key;
						cur.last_end_seq_id = end_ids[0];
						cur.initialized = true;
					}
					continue;
				}

				/* If no new data, skip */
				if (latest_key <= cur.last_key) {
					continue;
				}

				/*
				 * 2. Fetch recent candles to find what's new.
				 * We fetch a larger batch (e.g., 32) to cover gaps.
				 */
				ret = trcache_get_candles_by_symbol_id_and_offset(
						cache, sym_id, cfg_idx, &req, 0, 32, batch);
				
				if (ret != 0 || batch->num_candles == 0) continue;

				uint64_t read_ts = get_micros();
				
				/* Extract Column Arrays */
				uint64_t* c_ex_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_EXCHANGE_TS];
				uint64_t* c_loc_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_LOCAL_TS];
				uint64_t* c_start = (uint64_t*)
					batch->column_arrays[VAL_COL_START_SEQ_ID];
				uint64_t* c_end = (uint64_t*)
					batch->column_arrays[VAL_COL_END_SEQ_ID];

				/* Iterate through batch (Oldest -> Newest) */
				for (int i = 0; i < batch->num_candles; ++i) {
					/* Skip incomplete (open) candles */
					if (!batch->is_closed_array[i]) continue;

					/* Skip already processed candles */
					if (batch->key_array[i] <= cur.last_key) continue;

					busy = true;

					/* 1. Gap Detection */
					if (c_start[i] != cur.last_end_seq_id + 1) {
						stats.gap_count++;
					}

					/* 2. Latency Measurement */
					double engine_lat = 0;
					double total_lat = 0;
					
					/* Convert exchange ts (ms) to micros */
					uint64_t ex_ts_us = c_ex_ts[i] * 1000;

					if (c_loc_ts[i] > ex_ts_us)
						engine_lat = (double)(c_loc_ts[i] - ex_ts_us);
					
					if (read_ts > ex_ts_us)
						total_lat = (double)(read_ts - ex_ts_us);

					stats.add(engine_lat, total_lat);

					/* Update Cursor */
					cur.last_key = batch->key_array[i];
					cur.last_end_seq_id = c_end[i];
				}
			}
		}

		/* Reporting */
		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::seconds>(
			now - last_report).count() >= 1) {
			stats.report_and_reset();
			last_report = now;
		}

		if (!busy) {
			std::this_thread::sleep_for(
				std::chrono::milliseconds(1));
		}
	}

	for (auto* b : batches) {
		trcache_batch_free(b);
	}
	std::cout << "[Auditor] Thread stopped." << std::endl;
}
