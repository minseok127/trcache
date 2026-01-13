/*
 * validator/core/auditor.cpp
 *
 * Implementation of the Auditor module.
 *
 * This module acts as the quality assurance layer for the validator.
 * It performs two key verification tasks:
 * 1. Internal Integrity: Checks for sequence gaps in candle trades.
 * 2. System Latency: Measures Feed, Internal, and Audit Latency.
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
	/* Interval Data (Reset every report) */
	std::vector<double> feed_samples;
	std::vector<double> internal_samples;
	std::vector<double> audit_samples;
	
	/* Cumulative Counters (Never reset) */
	uint64_t total_candles;
	uint64_t total_gaps;
	uint64_t total_tick_errors;

	/* Global Stats for Final Report */
	double global_feed_sum;
	double global_feed_max;
	double global_int_sum;
	double global_int_max;
	double global_aud_sum;
	double global_aud_max;
	uint64_t global_count;

	latency_stats() : 
		total_candles(0), total_gaps(0), total_tick_errors(0),
		global_feed_sum(0), global_feed_max(0),
		global_int_sum(0), global_int_max(0),
		global_aud_sum(0), global_aud_max(0),
		global_count(0)
	{
		reserve_vectors();
	}

	void reserve_vectors() {
		feed_samples.reserve(4096);
		internal_samples.reserve(4096);
		audit_samples.reserve(4096);
	}

	void add(double feed, double internal, double audit,
		 bool has_gap, bool has_tick_err) {
		
		/* Interval Samples */
		feed_samples.push_back(feed);
		internal_samples.push_back(internal);
		audit_samples.push_back(audit);

		/* Cumulative Counters */
		total_candles++;
		if (has_gap) total_gaps++;
		if (has_tick_err) total_tick_errors++;

		/* Global Stats Update */
		global_count++;
		
		global_feed_sum += feed;
		if (feed > global_feed_max) global_feed_max = feed;

		global_int_sum += internal;
		if (internal > global_int_max) global_int_max = internal;

		global_aud_sum += audit;
		if (audit > global_aud_max) global_aud_max = audit;
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

	/* Reports interval latency but cumulative counts */
	void report_interval() {
		if (feed_samples.empty()) return;

		double f_avg = get_avg(feed_samples);
		double f_p99 = get_p99(feed_samples);
		double f_max = get_max(feed_samples);

		double i_avg = get_avg(internal_samples);
		double i_p99 = get_p99(internal_samples);
		double i_max = get_max(internal_samples);

		double a_avg = get_avg(audit_samples);
		double a_p99 = get_p99(audit_samples);
		double a_max = get_max(audit_samples);

		/* * Print Cumulative Counts first to see progress.
		 * Then print Interval Latency to see current health.
		 */
		std::cout << "[Auditor] Total: " << total_candles
			  << " | Gaps: " << total_gaps
			  << " | TickErr: " << total_tick_errors << "\n"
			  << std::fixed << std::setprecision(3)
			  << "   [Feed Latency] Avg: " << f_avg / 1000.0
			  << " us | P99: " << f_p99 / 1000.0
			  << " us | Max: " << f_max / 1000.0 << " us\n"
			  << "   [Int  Latency] Avg: " << i_avg / 1000.0
			  << " us | P99: " << i_p99 / 1000.0
			  << " us | Max: " << i_max / 1000.0 << " us\n"
			  << "   [Aud  Latency] Avg: " << a_avg / 1000.0
			  << " us | P99: " << a_p99 / 1000.0
			  << " us | Max: " << a_max / 1000.0 << " us"
			  << std::endl;

		/* Reset Interval Data Only */
		feed_samples.clear();
		internal_samples.clear();
		audit_samples.clear();
		reserve_vectors();
	}

	void report_final() {
		if (global_count == 0) return;

		double f_avg = global_feed_sum / global_count;
		double i_avg = global_int_sum / global_count;
		double a_avg = global_aud_sum / global_count;

		std::cout << "\n========================================\n"
			  << "        VALIDATOR FINAL REPORT          \n"
			  << "========================================\n"
			  << " Total Candles Processed : " << total_candles << "\n"
			  << " Total Sequence Gaps     : " << total_gaps << "\n"
			  << " Total Tick Count Errors : " << total_tick_errors << "\n"
			  << "----------------------------------------\n"
			  << std::fixed << std::setprecision(3)
			  << " [Feed Latency] Avg: " << f_avg / 1000.0
			  << " us | Max: " << global_feed_max / 1000.0 << " us\n"
			  << " [Int  Latency] Avg: " << i_avg / 1000.0
			  << " us | Max: " << global_int_max / 1000.0 << " us\n"
			  << " [Aud  Latency] Avg: " << a_avg / 1000.0
			  << " us | Max: " << global_aud_max / 1000.0 << " us\n"
			  << "========================================\n"
			  << std::endl;
	}
};

/*
 * --------------------------------------------------------------------------
 * Helper Functions
 * --------------------------------------------------------------------------
 */

/* Get current time in nanoseconds */
static inline uint64_t get_current_ns()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
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

	/* Prepare Field Request - Include FEED_TS */
	int req_indices[] = {
		VAL_COL_EXCHANGE_TS,
		VAL_COL_FEED_TS,
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
		bool busy = false;

		for (int sym_id = 0; sym_id < max_symbols; ++sym_id) {
			for (int cfg_idx = 0; cfg_idx < num_configs; ++cfg_idx) {
				
				int idx = (sym_id * num_configs) + cfg_idx;
				symbol_cursor& cur = cursors[idx];
				trcache_candle_batch* batch = batches[cfg_idx];
				
				const auto& candle_cfg = config.candles[cfg_idx];
				bool is_tick_candle = (candle_cfg.type == "TICK_MODULO");
				uint64_t threshold = candle_cfg.threshold;
				if (threshold == 0) threshold = 1;

				/* 1. Check Latest */
				int ret = trcache_get_candles_by_symbol_id_and_offset(
						cache, sym_id, cfg_idx, &req,
						0, 1, batch);
				
				if (ret != 0 || batch->num_candles == 0) {
					continue;
				}

				uint64_t latest_key = batch->key_array[0];
				
				/* Initialization */
				if (!cur.initialized) {
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

				if (latest_key <= cur.last_key) {
					continue;
				}

				/* 2. Fetch specific range */
				uint64_t diff = latest_key - cur.last_key;
				int count = diff / threshold;
				
				if (count > 128) count = 128;
				if (count < 1) count = 1;

				ret = trcache_get_candles_by_symbol_id_and_key(
						cache, sym_id, cfg_idx, &req,
						latest_key, count, batch);
				
				if (ret != 0 || batch->num_candles == 0) continue;

				uint64_t read_ts = get_current_ns();
				
				uint64_t* c_ex_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_EXCHANGE_TS];
				uint64_t* c_feed_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_FEED_TS];
				uint64_t* c_loc_ts = (uint64_t*)
					batch->column_arrays[VAL_COL_LOCAL_TS];
				uint64_t* c_start = (uint64_t*)
					batch->column_arrays[VAL_COL_START_SEQ_ID];
				uint64_t* c_end = (uint64_t*)
					batch->column_arrays[VAL_COL_END_SEQ_ID];
				uint64_t* c_tick_cnt = (uint64_t*)
					batch->column_arrays[VAL_COL_TICK_COUNT];

				for (int i = 0; i < batch->num_candles; ++i) {
					if (!batch->is_closed_array[i]) continue;
					if (batch->key_array[i] <= cur.last_key)
						continue;

					busy = true;
					bool has_gap = false;
					bool has_tick_err = false;

					/* Gap Check */
					if (c_start[i] != cur.last_end_seq_id + 1) {
						has_gap = true;
						/* Print gap warning only once in a while */
						if (stats.total_gaps % 100 == 0) {
							std::cerr << "[Auditor] GAP | "
								  << "Exp: " << cur.last_end_seq_id + 1
								  << " Act: " << c_start[i]
								  << std::endl;
						}
					}

					/* Tick Count Check */
					if (is_tick_candle) {
						if (c_tick_cnt[i] != threshold) {
							has_tick_err = true;
						}
					}

					/* Latency Calc */
					int64_t lat_feed = (int64_t)c_feed_ts[i] - 
							   (int64_t)c_ex_ts[i];
					int64_t lat_int = (int64_t)c_loc_ts[i] - 
							  (int64_t)c_feed_ts[i];
					int64_t lat_aud = (int64_t)read_ts - 
							  (int64_t)c_loc_ts[i];

					if (lat_feed < 0) lat_feed = 0;
					if (lat_int < 0) lat_int = 0;
					if (lat_aud < 0) lat_aud = 0;

					stats.add((double)lat_feed,
						  (double)lat_int,
						  (double)lat_aud,
						  has_gap, has_tick_err);

					cur.last_key = batch->key_array[i];
					cur.last_end_seq_id = c_end[i];
				}
			}
		}

		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::seconds>(
			now - last_report).count() >= 1) {
			stats.report_interval();
			last_report = now;
		}

		if (!busy) {
			std::this_thread::sleep_for(
				std::chrono::milliseconds(1));
		}
	}

	/* Final Summary on Exit */
	stats.report_final();

	for (auto* b : batches) {
		trcache_batch_free(b);
	}
	std::cout << "[Auditor] Thread stopped." << std::endl;
}
