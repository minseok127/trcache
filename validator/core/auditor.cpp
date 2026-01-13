/*
 * validator/core/auditor.cpp
 *
 * Implementation of the Auditor module with Hybrid Statistics.
 *
 * This module acts as the quality assurance layer for the validator.
 * It performs two key verification tasks:
 * 1. Internal Integrity: Checks for sequence gaps in candle trades.
 * 2. System Latency: Measures Network+Parsing, Engine, and Detection Latency.
 * 3. Tick Count Integrity: Verifies tick counts for TICK-based candles.
 * 4. CSV Reporting: Saves interval statistics to a file for analysis.
 *
 * [Statistics Strategy]
 * - Window Stats (Current): std::vector (Exact precision, Reset every interval)
 * - Global Stats (Total): SimpleHistogram (Fixed-bin, Accumulated forever)
 * - Metric Change: Replaced Average (Mean) with Median (P50) to better
 * represent typical performance in long-tail distributions.
 */

#include "auditor.h"
#include "types.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <cstring>
#include <time.h>
#include <iomanip>
#include <sstream>
#include <cmath>

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

/*
 * --------------------------------------------------------------------------
 * Global Histogram (Log-Scaled Bins)
 * --------------------------------------------------------------------------
 *
 * Log-scaled fixed-bin histogram for latency measurement.
 * - Stores nanoseconds (int64)
 * - Piecewise precision to keep memory small while preserving tail accuracy.
 *
 * Bucket layout:
 *   [0,    1ms)    step 100ns   -> 10,000 buckets
 *   [1ms,  10ms)   step 1us     -> 9,000 buckets
 *   [10ms, 100ms)  step 10us    -> 9,000 buckets
 *   [100ms, 1s)    step 100us   -> 9,000 buckets
 *   overflow bucket (>= 1s)
 *
 * Total buckets: 37,001 (about 296KB per instance with uint64_t)
 */
struct LogHistogram {
	static constexpr int64_t MS1_NS = 1000LL * 1000;
	static constexpr int64_t MS10_NS = 10LL * 1000 * 1000;
	static constexpr int64_t MS100_NS = 100LL * 1000 * 1000;
	static constexpr int64_t S1_NS = 1000LL * 1000 * 1000;

	static constexpr int64_t STEP_100NS = 100;
	static constexpr int64_t STEP_1US = 1000;
	static constexpr int64_t STEP_10US = 10LL * 1000;
	static constexpr int64_t STEP_100US = 100LL * 1000;

	static constexpr int STAGE_COUNT = 4;

	int64_t base_ns[STAGE_COUNT];
	int64_t limit_ns[STAGE_COUNT];
	int64_t step_ns[STAGE_COUNT];
	int bucket_off[STAGE_COUNT];
	int bucket_cnt[STAGE_COUNT];

	std::vector<uint64_t> buckets;
	uint64_t total_count;
	int64_t max_val;

	LogHistogram()
		: total_count(0), max_val(0)
	{
		init_layout();
	}

	void init_layout() {
		base_ns[0] = 0;
		limit_ns[0] = MS1_NS;
		step_ns[0] = STEP_100NS;

		base_ns[1] = MS1_NS;
		limit_ns[1] = MS10_NS;
		step_ns[1] = STEP_1US;

		base_ns[2] = MS10_NS;
		limit_ns[2] = MS100_NS;
		step_ns[2] = STEP_10US;

		base_ns[3] = MS100_NS;
		limit_ns[3] = S1_NS;
		step_ns[3] = STEP_100US;

		int off = 0;
		for (int s = 0; s < STAGE_COUNT; ++s) {
			int64_t span = limit_ns[s] - base_ns[s];
			bucket_cnt[s] = (int)(span / step_ns[s]);
			bucket_off[s] = off;
			off += bucket_cnt[s];
		}

		/* +1 for overflow bucket */
		buckets.assign((size_t)off + 1, 0);
	}

	void record(int64_t val_ns) {
		if (val_ns < 0) return;

		if (val_ns > max_val) max_val = val_ns;

		int idx = bucket_index(val_ns);
		buckets[(size_t)idx]++;
		total_count++;
	}

	int bucket_index(int64_t val_ns) const {
		for (int s = 0; s < STAGE_COUNT; ++s) {
			if (val_ns < limit_ns[s]) {
				int64_t rel = val_ns - base_ns[s];
				int bin = (int)(rel / step_ns[s]);
				if (bin < 0) bin = 0;
				if (bin >= bucket_cnt[s]) bin = bucket_cnt[s] - 1;
				return bucket_off[s] + bin;
			}
		}

		/* Overflow bucket */
		return (int)(buckets.size() - 1);
	}

	int64_t value_from_index(int idx) const {
		int last = (int)buckets.size() - 1;
		if (idx >= last) return max_val;

		for (int s = 0; s < STAGE_COUNT; ++s) {
			int off = bucket_off[s];
			int cnt = bucket_cnt[s];
			if (idx >= off && idx < off + cnt) {
				int rel = idx - off;
				return base_ns[s] + (int64_t)rel * step_ns[s];
			}
		}

		return max_val;
	}

	int64_t percentile(double p) const {
		if (total_count == 0) return 0;

		if (p >= 100.0) return max_val;
		if (p <= 0.0) return 0;

		double frac = p / 100.0;
		uint64_t rank = (uint64_t)std::ceil(frac * (double)total_count);
		if (rank < 1) rank = 1;

		uint64_t accum = 0;
		int last = (int)buckets.size();

		for (int i = 0; i < last; ++i) {
			accum += buckets[(size_t)i];
			if (accum >= rank) {
				return value_from_index(i);
			}
		}

		return max_val;
	}
};

struct latency_stats {
	/* 1. Window Data (Reset every report) */
	std::vector<double> feed_samples;
	std::vector<double> internal_samples;
	std::vector<double> audit_samples;

	/* 2. Global Data (Accumulate forever) */
	LogHistogram global_feed_hist;
	LogHistogram global_int_hist;
	LogHistogram global_aud_hist;

	/* Cumulative Counters */
	uint64_t total_candles;
	uint64_t total_gaps;
	uint64_t total_tick_errors;

	latency_stats() :
		total_candles(0), total_gaps(0), total_tick_errors(0)
	{
		reserve_vectors();
	}

	/* SimpleHistogram handles its own memory, no special dtor needed */
	~latency_stats() {}

	void reserve_vectors() {
		feed_samples.reserve(4096);
		internal_samples.reserve(4096);
		audit_samples.reserve(4096);
	}

	void add(double feed_ns, double internal_ns, double audit_ns,
		 bool has_gap, bool has_tick_err) {

		/* Interval Samples */
		feed_samples.push_back(feed_ns);
		internal_samples.push_back(internal_ns);
		audit_samples.push_back(audit_ns);

		/* Global Histograms (Record as int64 nanoseconds) */
		global_feed_hist.record((int64_t)feed_ns);
		global_int_hist.record((int64_t)internal_ns);
		global_aud_hist.record((int64_t)audit_ns);

		/* Cumulative Counters */
		total_candles++;
		if (has_gap) total_gaps++;
		if (has_tick_err) total_tick_errors++;
	}

	/* Helper struct for passing stats (Replaced avg with p50) */
	struct vec_stats {
		double p50;   /* Median */
		double p99;   /* 99th percentile */
		double p999;  /* 99.9th percentile */
		double max;   /* Maximum value */
	};

	vec_stats calc_vec_stats(std::vector<double>& v) {
		if (v.empty()) return {0, 0, 0, 0};

		/* Sorting is required for percentiles */
		std::sort(v.begin(), v.end());

		double p50 = v[v.size() * 0.50];
		double p99 = v[v.size() * 0.99];
		double p999 = v[v.size() * 0.999];
		double max = v.back();

		return {p50, p99, p999, max};
	}

	vec_stats get_simple_stats(LogHistogram& h) {
		return vec_stats{
			(double)h.percentile(50.0),
			(double)h.percentile(99.0),
			(double)h.percentile(99.9),
			(double)h.max_val
		};
	}

	/* Reports interval latency and writes to CSV if stream is open */
	void report_interval(std::ofstream* csv_file) {
		if (feed_samples.empty()) return;

		/* 1. Calculate Window Stats (Current) */
		vec_stats f_win = calc_vec_stats(feed_samples);
		vec_stats i_win = calc_vec_stats(internal_samples);
		vec_stats a_win = calc_vec_stats(audit_samples);

		/* 2. Calculate Global Stats (Total) */
		vec_stats f_glb = get_simple_stats(global_feed_hist);
		vec_stats i_glb = get_simple_stats(global_int_hist);
		vec_stats a_glb = get_simple_stats(global_aud_hist);

		/* Console Output - Replaced Avg with P50 */
		std::cout << "[Auditor] Candles: " << total_candles
			  << " | Gaps: " << total_gaps << "\n"
			  << std::fixed << std::setprecision(3)

			  << "   [Network + Parsing]\n"
			  << "      Current: P50 " << f_win.p50/1000.0
			  << " | P99 " << f_win.p99/1000.0
			  << " | P99.9 " << f_win.p999/1000.0
			  << " | Max " << f_win.max/1000.0 << " us\n"
			  << "      Total  : P50 " << f_glb.p50/1000.0
			  << " | P99 " << f_glb.p99/1000.0
			  << " | P99.9 " << f_glb.p999/1000.0
			  << " | Max " << f_glb.max/1000.0 << " us\n"

			  << "   [Engine Internal]\n"
			  << "      Current: P50 " << i_win.p50/1000.0
			  << " | P99 " << i_win.p99/1000.0
			  << " | P99.9 " << i_win.p999/1000.0
			  << " | Max " << i_win.max/1000.0 << " us\n"
			  << "      Total  : P50 " << i_glb.p50/1000.0
			  << " | P99 " << i_glb.p99/1000.0
			  << " | P99.9 " << i_glb.p999/1000.0
			  << " | Max " << i_glb.max/1000.0 << " us\n"

			  << "   [Auditor Detection]\n"
			  << "      Current: P50 " << a_win.p50/1000.0
			  << " | P99 " << a_win.p99/1000.0
			  << " | P99.9 " << a_win.p999/1000.0
			  << " | Max " << a_win.max/1000.0 << " us\n"
			  << "      Total  : P50 " << a_glb.p50/1000.0
			  << " | P99 " << a_glb.p99/1000.0
			  << " | P99.9 " << a_glb.p999/1000.0
			  << " | Max " << a_glb.max/1000.0 << " us"
			  << std::endl;

		/* CSV Output - Updated columns to use Median */
		if (csv_file && csv_file->is_open()) {
			auto now = std::chrono::system_clock::now();
			std::time_t t_now = std::chrono::system_clock::to_time_t(now);
			struct tm* ptm = std::localtime(&t_now);
			char time_buf[32];
			std::strftime(time_buf, sizeof(time_buf),
				      "%Y-%m-%d %H:%M:%S", ptm);

			*csv_file << time_buf << ","
				  << total_candles << ","
				  << total_gaps << ","
				  /* Window Stats */
				  << f_win.p50 << "," << f_win.p99 << ","
				  << f_win.p999 << "," << f_win.max << ","
				  << i_win.p50 << "," << i_win.p99 << ","
				  << i_win.p999 << "," << i_win.max << ","
				  << a_win.p50 << "," << a_win.p99 << ","
				  << a_win.p999 << "," << a_win.max << ","
				  /* Global Stats */
				  << f_glb.p50 << "," << f_glb.p99 << ","
				  << f_glb.p999 << "," << f_glb.max << ","
				  << i_glb.p50 << "," << i_glb.p99 << ","
				  << i_glb.p999 << "," << i_glb.max << ","
				  << a_glb.p50 << "," << a_glb.p99 << ","
				  << a_glb.p999 << "," << a_glb.max
				  << "\n";
		}

		/* Reset Window Data Only */
		feed_samples.clear();
		internal_samples.clear();
		audit_samples.clear();
		reserve_vectors();
	}

	void report_final() {
		if (global_feed_hist.total_count == 0) return;

		std::cout << "\n[Auditor] Calculating Final Statistics..."
			  << std::endl;

		vec_stats f = get_simple_stats(global_feed_hist);
		vec_stats i = get_simple_stats(global_int_hist);
		vec_stats a = get_simple_stats(global_aud_hist);

		std::cout << "\n========================================\n"
			  << "        VALIDATOR FINAL REPORT          \n"
			  << "========================================\n"
			  << " Total Candles Processed : " << total_candles << "\n"
			  << " Total Sequence Gaps     : " << total_gaps << "\n"
			  << " Total Tick Count Errors : " << total_tick_errors << "\n"
			  << "----------------------------------------\n"
			  << std::fixed << std::setprecision(3)
			  << " [Network + Parsing Latency]\n"
			  << "   P50: " << f.p50 / 1000.0 << " us\n"
			  << "   P99: " << f.p99 / 1000.0 << " us\n"
			  << " P99.9: " << f.p999 / 1000.0 << " us\n"
			  << "   Max: " << f.max / 1000.0 << " us\n"
			  << " [Engine Internal Latency]\n"
			  << "   P50: " << i.p50 / 1000.0 << " us\n"
			  << "   P99: " << i.p99 / 1000.0 << " us\n"
			  << " P99.9: " << i.p999 / 1000.0 << " us\n"
			  << "   Max: " << i.max / 1000.0 << " us\n"
			  << " [Auditor Detection Latency]\n"
			  << "   P50: " << a.p50 / 1000.0 << " us\n"
			  << "   P99: " << a.p99 / 1000.0 << " us\n"
			  << " P99.9: " << a.p999 / 1000.0 << " us\n"
			  << "   Max: " << a.max / 1000.0 << " us\n"
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

			/* CSV Header - Changed Avg to P50 */
			csv_file << "Time,Total_Candles,Gaps,"
				 << "Win_Feed_P50,Win_Feed_P99,Win_Feed_P999,"
				 << "Win_Feed_Max,"
				 << "Win_Int_P50,Win_Int_P99,Win_Int_P999,"
				 << "Win_Int_Max,"
				 << "Win_Aud_P50,Win_Aud_P99,Win_Aud_P999,"
				 << "Win_Aud_Max,"
				 << "Glb_Feed_P50,Glb_Feed_P99,Glb_Feed_P999,"
				 << "Glb_Feed_Max,"
				 << "Glb_Int_P50,Glb_Int_P99,Glb_Int_P999,"
				 << "Glb_Int_Max,"
				 << "Glb_Aud_P50,Glb_Aud_P99,Glb_Aud_P999,"
				 << "Glb_Aud_Max"
				 << "\n";
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
