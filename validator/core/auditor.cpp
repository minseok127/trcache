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

/*
 * --------------------------------------------------------------------------
 * Internal Structures
 * --------------------------------------------------------------------------
 */

struct symbol_cursor {
	uint64_t next_key;
	uint64_t last_end_seq_id;
	bool initialized;

	symbol_cursor() : next_key(0), last_end_seq_id(0), initialized(false) {}
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
			  << " | Eng Lat(us) Avg: " << (long)eng_avg
			  << " P99: " << (long)eng_p99
			  << " Max: " << (long)eng_max
			  << " | Tot Lat(us) Avg: " << (long)tot_avg
			  << " P99: " << (long)tot_p99
			  << " Max: " << (long)tot_max
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
		batches[i] = trcache_batch_alloc_on_heap(cache, i, 1, &req);
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
				int threshold = config.candles[cfg_idx].threshold;

				/* Synchronization Step */
				if (!cur.initialized) {
					int ret =
					trcache_get_candles_by_symbol_id_and_offset(
						cache, sym_id, cfg_idx, &req,
						0, 1, batch);
					
					if (ret == 0 && batch->num_candles > 0 &&
					    batch->is_closed_array[0]) {
						cur.next_key =
							batch->key_array[0] +
							threshold;
						uint64_t* end_ids = (uint64_t*)
						batch->column_arrays[VAL_COL_END_SEQ_ID];
						cur.last_end_seq_id = end_ids[0];
						cur.initialized = true;
					}
					continue;
				}

				/* Sequential Polling Step */
				int ret =
				trcache_get_candles_by_symbol_id_and_key(
					cache, sym_id, cfg_idx, &req,
					cur.next_key, 1, batch);

				if (ret != 0 || batch->num_candles == 0 ||
				    !batch->is_closed_array[0]) {
					continue;
				}

				uint64_t read_ts = get_micros();
				busy = true;

				/* Extract Column Data */
				uint64_t* c_ex_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_EXCHANGE_TS];
				uint64_t* c_loc_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_LOCAL_TS];
				uint64_t* c_start = (uint64_t*)
					batch->column_arrays[VAL_COL_START_SEQ_ID];
				uint64_t* c_end = (uint64_t*)
					batch->column_arrays[VAL_COL_END_SEQ_ID];

				/* 1. Gap Detection */
				if (c_start[0] != cur.last_end_seq_id + 1) {
					stats.gap_count++;
				}

				/* 2. Latency Measurement */
				double engine_lat = 0;
				double total_lat = 0;
				if (c_loc_ts[0] > c_ex_ts[0])
					engine_lat = (double)(c_loc_ts[0] -
							      c_ex_ts[0]);
				if (read_ts > c_ex_ts[0])
					total_lat = (double)(read_ts -
							     c_ex_ts[0]);
				
				stats.add(engine_lat, total_lat);

				/* Update State */
				cur.last_end_seq_id = c_end[0];
				cur.next_key += threshold;
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
