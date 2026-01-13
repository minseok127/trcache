/*
 * validator/core/auditor.cpp
 *
 * Implementation of the Auditor module.
 *
 * This module acts as the quality assurance layer for the validator.
 * It performs two key verification tasks:
 * 1. Internal Integrity: Checks for sequence gaps in candle trades.
 * 2. System Latency: Measures Network+Parsing, Engine, and Audit Latency.
 * 3. Tick Count Integrity: Verifies tick counts for TICK-based candles.
 * 4. CSV Reporting: Saves interval statistics to a file for analysis.
 */

#include "auditor.h"
#include "types.h"

#include <iostream>
#include <fstream> /* Added for CSV output */
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <cstring>
#include <time.h>
#include <iomanip>
#include <sstream>

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
	
	/* Global Data (Accumulate all samples for Final P99) */
	std::vector<double> global_feed_samples;
	std::vector<double> global_int_samples;
	std::vector<double> global_aud_samples;

	/* Cumulative Counters */
	uint64_t total_candles;
	uint64_t total_gaps;
	uint64_t total_tick_errors;

	latency_stats() : 
		total_candles(0), total_gaps(0), total_tick_errors(0)
	{
		reserve_vectors();
	}

	void reserve_vectors() {
		feed_samples.reserve(4096);
		internal_samples.reserve(4096);
		audit_samples.reserve(4096);
		
		/* Global vectors will grow large, reserve initial chunk */
		global_feed_samples.reserve(100000);
		global_int_samples.reserve(100000);
		global_aud_samples.reserve(100000);
	}

	void add(double feed, double internal, double audit,
		 bool has_gap, bool has_tick_err) {
		
		/* Interval Samples */
		feed_samples.push_back(feed);
		internal_samples.push_back(internal);
		audit_samples.push_back(audit);

		/* Global Samples (For Final P99) */
		global_feed_samples.push_back(feed);
		global_int_samples.push_back(internal);
		global_aud_samples.push_back(audit);

		/* Cumulative Counters */
		total_candles++;
		if (has_gap) total_gaps++;
		if (has_tick_err) total_tick_errors++;
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

	double get_max(const std::vector<double>& v) {
		if (v.empty()) return 0;
		double max_val = 0;
		for (double d : v) {
			if (d > max_val) max_val = d;
		}
		return max_val;
	}

	/* Reports interval latency and writes to CSV if stream is open */
	void report_interval(std::ofstream* csv_file) {
		if (feed_samples.empty()) return;

		double f_avg = get_avg(feed_samples);
		double f_p99 = get_p99(feed_samples); /* Sorts feed_samples */
		double f_max = feed_samples.back();   /* After sort, last is max */

		double i_avg = get_avg(internal_samples);
		double i_p99 = get_p99(internal_samples);
		double i_max = internal_samples.back();

		double a_avg = get_avg(audit_samples);
		double a_p99 = get_p99(audit_samples);
		double a_max = audit_samples.back();

		/* Console Output */
		std::cout << "[Auditor] Total: " << total_candles
			  << " | Gaps: " << total_gaps
			  << " | TickErr: " << total_tick_errors << "\n"
			  << std::fixed << std::setprecision(3)
			  << "   [Network + Parsing Latency] Avg: " << f_avg / 1000.0
			  << " us | P99: " << f_p99 / 1000.0
			  << " us | Max: " << f_max / 1000.0 << " us\n"
			  << "   [Engine Internal Latency] Avg: " << i_avg / 1000.0
			  << " us | P99: " << i_p99 / 1000.0
			  << " us | Max: " << i_max / 1000.0 << " us\n"
			  << "   [Auditor Detection Latency] Avg: " << a_avg / 1000.0
			  << " us | P99: " << a_p99 / 1000.0
			  << " us | Max: " << a_max / 1000.0 << " us"
			  << std::endl;

		/* CSV Output (No flush for performance) */
		if (csv_file && csv_file->is_open()) {
			auto now = std::chrono::system_clock::now();
			std::time_t t_now = std::chrono::system_clock::
						to_time_t(now);
			struct tm* ptm = std::localtime(&t_now);
			char time_buf[32];
			std::strftime(time_buf, sizeof(time_buf), 
				      "%Y-%m-%d %H:%M:%S", ptm);

			*csv_file << time_buf << ","
				  << total_candles << ","
				  << total_gaps << ","
				  << f_avg << "," << f_p99 << "," << f_max << ","
				  << i_avg << "," << i_p99 << "," << i_max << ","
				  << a_avg << "," << a_p99 << "," << a_max 
				  << "\n";
		}

		/* Reset Interval Data Only */
		feed_samples.clear();
		internal_samples.clear();
		audit_samples.clear();
		
		/* Re-reserve interval vectors */
		feed_samples.reserve(4096);
		internal_samples.reserve(4096);
		audit_samples.reserve(4096);
	}

	void report_final() {
		if (global_feed_samples.empty()) return;

		std::cout << "\n[Auditor] Calculating Final Statistics..." 
			  << std::endl;

		double f_avg = get_avg(global_feed_samples);
		double f_p99 = get_p99(global_feed_samples);
		double f_max = global_feed_samples.back();

		double i_avg = get_avg(global_int_samples);
		double i_p99 = get_p99(global_int_samples);
		double i_max = global_int_samples.back();

		double a_avg = get_avg(global_aud_samples);
		double a_p99 = get_p99(global_aud_samples);
		double a_max = global_aud_samples.back();

		std::cout << "\n========================================\n"
			  << "        VALIDATOR FINAL REPORT          \n"
			  << "========================================\n"
			  << " Total Candles Processed : " << total_candles << "\n"
			  << " Total Sequence Gaps     : " << total_gaps << "\n"
			  << " Total Tick Count Errors : " << total_tick_errors << "\n"
			  << "----------------------------------------\n"
			  << std::fixed << std::setprecision(3)
			  << " [Network + Parsing Latency]\n"
			  << "   Avg: " << f_avg / 1000.0 << " us\n"
			  << "   P99: " << f_p99 / 1000.0 << " us\n"
			  << "   Max: " << f_max / 1000.0 << " us\n"
			  << " [Engine Internal Latency]\n"
			  << "   Avg: " << i_avg / 1000.0 << " us\n"
			  << "   P99: " << i_p99 / 1000.0 << " us\n"
			  << "   Max: " << i_max / 1000.0 << " us\n"
			  << " [Auditor Detection Latency]\n"
			  << "   Avg: " << a_avg / 1000.0 << " us\n"
			  << "   P99: " << a_p99 / 1000.0 << " us\n"
			  << "   Max: " << a_max / 1000.0 << " us\n"
			  << "========================================\n"
			  << std::endl;
	}
};

/*
 * --------------------------------------------------------------------------
 * Helper Functions
 * --------------------------------------------------------------------------
 */

static inline uint64_t get_current_ns()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static std::string get_timestamp_string()
{
	auto now = std::chrono::system_clock::now();
	std::time_t t_now = std::chrono::system_clock::to_time_t(now);
	struct tm* ptm = std::localtime(&t_now);
	char buf[32];
	std::strftime(buf, sizeof(buf), "%Y%m%d_%H%M%S", ptm);
	return std::string(buf);
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

	/* 1. CSV Initialization */
	std::ofstream csv_file;
	if (!config.csv_output_path.empty()) {
		std::string final_path = config.csv_output_path;
		if (config.csv_append_timestamp) {
			/* Insert timestamp before extension */
			size_t ext_pos = final_path.find_last_of(".");
			std::string ts = "_" + get_timestamp_string();
			if (ext_pos != std::string::npos) {
				final_path.insert(ext_pos, ts);
			} else {
				final_path += ts;
			}
		}

		csv_file.open(final_path);
		if (csv_file.is_open()) {
			std::cout << "[Auditor] Saving report to: " 
				  << final_path << std::endl;
			
			/* Write CSV Header */
			csv_file << "Time,Total_Candles,Gaps,"
				 << "Feed_Avg_ns,Feed_P99_ns,Feed_Max_ns,"
				 << "Int_Avg_ns,Int_P99_ns,Int_Max_ns,"
				 << "Aud_Avg_ns,Aud_P99_ns,Aud_Max_ns\n";
		} else {
			std::cerr << "[Auditor] Failed to open CSV file: "
				  << final_path << std::endl;
		}
	}

	int num_configs = config.candles.size();
	int max_symbols = config.top_n + 50;
	int total_streams = max_symbols * num_configs;

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

					bool has_gap = false;
					bool has_tick_err = false;

					if (c_start[i] != cur.last_end_seq_id + 1) {
						has_gap = true;
						if (stats.total_gaps % 100 == 0) {
							std::cerr << "[Auditor] GAP | "
								  << "Exp: " << cur.last_end_seq_id + 1
								  << " Act: " << c_start[i]
								  << std::endl;
						}
					}

					if (is_tick_candle) {
						if (c_tick_cnt[i] != threshold) {
							has_tick_err = true;
						}
					}

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
			/* Pass file pointer to reporting function */
			stats.report_interval(&csv_file);
			last_report = now;
		}
	}

	/* Final Cleanup */
	if (csv_file.is_open()) {
		csv_file.flush();
		csv_file.close();
		std::cout << "[Auditor] CSV report saved." << std::endl;
	}

	stats.report_final();

	for (auto* b : batches) {
		trcache_batch_free(b);
	}
	std::cout << "[Auditor] Thread stopped." << std::endl;
}
