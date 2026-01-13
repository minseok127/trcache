/*
 * validator/core/auditor.cpp
 *
 * Implementation of the Auditor module.
 *
 * This module acts as the quality assurance layer for the validator.
 * It performs two key verification tasks:
 * 1. Internal Integrity: Checks for sequence gaps in candle trades.
 * 2. System Latency: Measures Feed, Internal, and Audit Latency in Nanoseconds.
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
	/* Separate vectors for 3-stage latency analysis */
	std::vector<double> feed_samples;     /* Exchange -> Engine Input */
	std::vector<double> internal_samples; /* Input -> Engine Close */
	std::vector<double> audit_samples;    /* Close -> Auditor Read */
	
	uint64_t gap_count;
	uint64_t tick_error_count;
	uint64_t total_candles;

	latency_stats() : gap_count(0), tick_error_count(0), total_candles(0) {
		feed_samples.reserve(4096);
		internal_samples.reserve(4096);
		audit_samples.reserve(4096);
	}

	void add(double feed, double internal, double audit) {
		feed_samples.push_back(feed);
		internal_samples.push_back(internal);
		audit_samples.push_back(audit);
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

		double f_avg = get_avg(feed_samples);
		double f_p99 = get_p99(feed_samples);
		double f_max = get_max(feed_samples);

		double i_avg = get_avg(internal_samples);
		double i_p99 = get_p99(internal_samples);
		double i_max = get_max(internal_samples);

		double a_avg = get_avg(audit_samples);
		double a_p99 = get_p99(audit_samples);
		double a_max = get_max(audit_samples);

		/* Report in Microseconds (us) with ns precision (.3f) */
		std::cout << "[Auditor] Candles: " << total_candles
			  << " | Gaps: " << gap_count
			  << " | TickErrs: " << tick_error_count << "\n"
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

		feed_samples.clear();
		internal_samples.clear();
		audit_samples.clear();
		feed_samples.reserve(4096);
		internal_samples.reserve(4096);
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
		bool busy = false;

		for (int sym_id = 0; sym_id < max_symbols; ++sym_id) {
			for (int cfg_idx = 0; cfg_idx < num_configs; ++cfg_idx) {
				
				int idx = (sym_id * num_configs) + cfg_idx;
				symbol_cursor& cur = cursors[idx];
				trcache_candle_batch* batch = batches[cfg_idx];
				
				const auto& candle_cfg = config.candles[cfg_idx];
				bool is_tick_candle = (candle_cfg.type == "TICK_MODULO");
				/* For TICK_MODULO, threshold is used for fetching calc */
				uint64_t threshold = candle_cfg.threshold;
				if (threshold == 0) threshold = 1;

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

				/*
				 * 2. Fetch specific range using Key Logic.
				 * Calculate how many candles are missing between 
				 * last_key and latest_key.
				 */
				uint64_t diff = latest_key - cur.last_key;
				/* We want the batch ENDING at latest_key */
				int count = diff / threshold;
				
				/* Cap count to batch size limit (128) */
				if (count > 128) count = 128;
				if (count < 1) count = 1;

				ret = trcache_get_candles_by_symbol_id_and_key(
						cache, sym_id, cfg_idx, &req,
						latest_key, count, batch);
				
				if (ret != 0 || batch->num_candles == 0) continue;

				uint64_t read_ts = get_current_ns();
				
				/* Extract Column Arrays */
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

				/* Iterate through batch (Oldest -> Newest) */
				for (int i = 0; i < batch->num_candles; ++i) {
					/* Skip incomplete (open) candles */
					if (!batch->is_closed_array[i]) continue;

					if (batch->key_array[i] <= cur.last_key)
						continue;

					busy = true;

					/* 1. Gap Detection */
					if (c_start[i] != cur.last_end_seq_id + 1) {
						stats.gap_count++;
						if (stats.gap_count <= 5) {
							std::cerr << "[Auditor] GAP | Sym: "
								  << sym_id
								  << " | Exp: "
								  << (cur.last_end_seq_id + 1)
								  << " | Act: " << c_start[i]
								  << std::endl;
						}
					}

					/* 2. Tick Count Validation */
					if (is_tick_candle) {
						if (c_tick_cnt[i] != threshold) {
							stats.tick_error_count++;
						}
					}

					/* 3. Latency Measurement (in Nanoseconds) */
					/*
					 * Feed Latency = Feed - Exchange
					 * Internal Latency = Local - Feed
					 * Audit Latency = Read - Local
					 */
					int64_t lat_feed = (int64_t)c_feed_ts[i] - 
							   (int64_t)c_ex_ts[i];
					int64_t lat_int = (int64_t)c_loc_ts[i] - 
							  (int64_t)c_feed_ts[i];
					int64_t lat_aud = (int64_t)read_ts - 
							  (int64_t)c_loc_ts[i];

					/* Sanity check */
					if (lat_feed < 0) lat_feed = 0;
					if (lat_int < 0) lat_int = 0;
					if (lat_aud < 0) lat_aud = 0;

					stats.add((double)lat_feed,
						  (double)lat_int,
						  (double)lat_aud);

					cur.last_key = batch->key_array[i];
					cur.last_end_seq_id = c_end[i];
				}
			}
		}

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
