/*
 * validator/core/auditor.cpp
 *
 * Implementation of the Auditor module.
 *
 * This module acts as the quality assurance layer for the validator.
 * It performs two key verification tasks:
 * 1. Internal Integrity: Checks for sequence gaps in candle trades.
 * 2. System Latency: Measures Engine Latency and Audit (Read) Latency.
 * 3. Tick Count Integrity: Verifies tick counts for TICK-based candles.
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
	std::vector<double> audit_samples;
	uint64_t gap_count;
	uint64_t tick_error_count; /* Added for tick count verification */
	uint64_t total_candles;

	latency_stats() : gap_count(0), tick_error_count(0), total_candles(0) {
		engine_samples.reserve(4096);
		audit_samples.reserve(4096);
	}

	void add(double engine_lat, double audit_lat) {
		engine_samples.push_back(engine_lat);
		audit_samples.push_back(audit_lat);
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

		double aud_avg = get_avg(audit_samples);
		double aud_p99 = get_p99(audit_samples);
		double aud_max = get_max(audit_samples);

		std::cout << "[Auditor] Processed: " << total_candles
			  << " | Gaps: " << gap_count
			  << " | TickErrs: " << tick_error_count
			  << std::fixed << std::setprecision(3)
			  << " | Eng Lat(millisecond) Avg: " << eng_avg / 1000000.0
			  << " P99: " << eng_p99 / 1000000.0
			  << " Max: " << eng_max / 1000000.0
			  << " | Audit Lat(microsecond) Avg: " << aud_avg / 1000.0
			  << " P99: " << aud_p99 / 1000.0
			  << " Max: " << aud_max / 1000.0
			  << std::endl;

		engine_samples.clear();
		audit_samples.clear();
		engine_samples.reserve(4096);
		audit_samples.reserve(4096);
		total_candles = 0;
		gap_count = 0;
		tick_error_count = 0;
	}
};

/*
 * --------------------------------------------------------------------------
 * Helper Functions
 * --------------------------------------------------------------------------
 */

static inline uint64_t get_nanos()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL +
	       (uint64_t)ts.tv_nsec;
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
		VAL_COL_END_SEQ_ID,
		VAL_COL_TICK_COUNT
	};
	trcache_field_request req = {
		req_indices,
		sizeof(req_indices) / sizeof(int)
	};

	/* Allocate Batches (One per config type) */
	std::vector<trcache_candle_batch*> batches(num_configs);
	for (int i = 0; i < num_configs; ++i) {
		/* Allocate enough buffer to fetch multiple candles at once */
		batches[i] = trcache_batch_alloc_on_heap(cache, i, 128, &req);
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

		for (int sym_id = 0; sym_id < max_symbols; ++sym_id) {
			for (int cfg_idx = 0; cfg_idx < num_configs; ++cfg_idx) {
				
				int idx = (sym_id * num_configs) + cfg_idx;
				symbol_cursor& cur = cursors[idx];
				trcache_candle_batch* batch = batches[cfg_idx];
				
				/* Check config for TICK type validation */
				const auto& candle_cfg = config.candles[cfg_idx];
				bool is_tick_candle = (candle_cfg.type == "TICK");
				uint64_t target_ticks = candle_cfg.threshold;

				/*
				 * 1. Check the latest candle (Offset 0) first.
				 */
				int ret = trcache_get_candles_by_symbol_id_and_offset(
						cache, sym_id, cfg_idx, &req,
						0, 1, batch);
				
				if (ret != 0 || batch->num_candles == 0) {
					continue;
				}

				uint64_t latest_key = batch->key_array[0];
				
				/* Initialization */
				if (!cur.initialized) {
					/* Wait for a closed candle to start */
					if (batch->is_closed_array[0]) {
						uint64_t* end_ids = (uint64_t*)
							batch->column_arrays
							[VAL_COL_END_SEQ_ID];
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
				 * 2. Fetch recent candle to find what's new.
				 */
				ret = trcache_get_candles_by_symbol_id_and_key(
						cache, sym_id, cfg_idx, &req,
						latest_key,
						(latest_key - cur.last_key) / candle_cfg.threshold,
						batch);
				
				if (ret != 0 || batch->num_candles == 0) continue;

				uint64_t read_ts = get_nanos();
				
				/* Extract Column Arrays */
				uint64_t* c_ex_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_EXCHANGE_TS];
				uint64_t* c_loc_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_LOCAL_TS];
				uint64_t* c_start = (uint64_t*)
					batch->column_arrays[VAL_COL_START_SEQ_ID];
				uint64_t* c_end = (uint64_t*)
					batch->column_arrays[VAL_COL_END_SEQ_ID];
				uint64_t* c_tick_cnt = (uint64_t*)
					batch->column_arrays[VAL_COL_TICK_COUNT];

				/* Iterate through batch (Oldest -> Newest) */
				for (int i = 0; i < batch->num_candles; ++i) {
					/* Skip incomplete (open) candles */
					if (!batch->is_closed_array[i]) continue;

					/* Skip already processed candles */
					if (batch->key_array[i] <= cur.last_key)
						continue;

					/* 1. Gap Detection */
					if (c_start[i] != cur.last_end_seq_id + 1) {
						stats.gap_count++;

						std::cerr << "[Auditor] GAP | Sym: " << sym_id
							  << " | Exp: " << (cur.last_end_seq_id + 1)
							  << " | Act: " << c_start[i]
							  << " | Diff: " 
							  << (c_start[i] - (cur.last_end_seq_id + 1))
							  << std::endl;
					}

					/* 2. Tick Count Validation (Requested) */
					if (is_tick_candle) {
						if (c_tick_cnt[i] != != candle_cfg.threshold) {
							stats.tick_error_count++;
							std::cerr << "[Auditor] TICK ERR | Sym: "
								  << sym_id
								  << " | Exp: " << candle_cfg.threshold
								  << " | Act: " << c_tick_cnt[i]
								  << std::endl;
						}
					}

					/* 3. Latency Measurement */
					uint64_t ex_ts_ns = c_ex_ts[i] * 1000000000;
					double engine_lat = (double)
						((int64_t)c_loc_ts[i] -
						 (int64_t)ex_ts_ns);
					double audit_lat = (double)
						((int64_t)read_ts -
						 (int64_t)c_loc_ts[i]);

					stats.add(engine_lat, audit_lat);

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
	}

	for (auto* b : batches) {
		trcache_batch_free(b);
	}
	std::cout << "[Auditor] Thread stopped." << std::endl;
}
