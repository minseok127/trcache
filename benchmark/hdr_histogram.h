/**
 * @file   hdr_histogram.h
 * @brief  Public interface for the HdrHistogram_c library.
 *
 * This file provides a simplified, self-contained version of the
 * HdrHistogram_c library, adapted to match the coding style of the
 * trcache project. It is intended for use within the benchmark program
 * to measure and analyze the distribution of latency values with high
 * fidelity.
 *
 * For production use, it is recommended to use the official library from:
 * https://github.com/HdrHistogram/HdrHistogram_c
 */
#ifndef HDR_HISTOGRAM_H
#define HDR_HISTOGRAM_H

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#if defined(__cplusplus)
extern "C" {
#endif

/**
 * @brief   Represents a High Dynamic Range (HDR) Histogram.
 *
 * Stores a count of recorded values in a compressed manner that provides
 * high resolution and a wide dynamic range.
 */
struct hdr_histogram
{
	int64_t lowest_trackable_value;
	int64_t highest_trackable_value;
	int32_t significant_figures;
	int32_t sub_bucket_count;
	int32_t bucket_count;
	int32_t counts_len;
	int64_t total_count;
	int64_t *counts;
};

/**
 * @brief   Allocate and initialize a new hdr_histogram.
 *
 * @param   lowest_trackable_value   The lowest value to be tracked.
 * @param   highest_trackable_value  The highest value to be tracked.
 * @param   significant_figures      The number of significant decimal digits.
 * @param   result                   A pointer to store the new histogram.
 *
 * @return  0 on success, -1 on failure.
 */
int hdr_histogram_init(
	int64_t lowest_trackable_value,
	int64_t highest_trackable_value,
	int significant_figures,
	struct hdr_histogram** result);

/**
 * @brief   Free the memory used by an hdr_histogram.
 * @param   h   The histogram to be freed.
 */
void hdr_histogram_close(struct hdr_histogram* h);

/**
 * @brief   Record a value in the histogram.
 * @param   h      The histogram.
 * @param   value  The value to record.
 * @return  true on success, false on failure (e.g., value out of range).
 */
bool hdr_histogram_record_value(struct hdr_histogram* h, int64_t value);

/**
 * @brief   Reset all counts in the histogram.
 * @param   h   The histogram to reset.
 */
void hdr_histogram_reset(struct hdr_histogram* h);

/**
 * @brief   Get the maximum value recorded in the histogram.
 * @param   h   The histogram.
 * @return  The maximum recorded value.
 */
int64_t hdr_histogram_max(const struct hdr_histogram* h);

/**
 * @brief   Get the mean of all recorded values.
 * @param   h   The histogram.
 * @return  The mean of the recorded values.
 */
double hdr_histogram_mean(const struct hdr_histogram* h);

/**
 * @brief   Get the value at a given percentile.
 * @param   h           The histogram.
 * @param   percentile  The percentile to look up (e.g., 99.9).
 * @return  The value at the specified percentile.
 */
int64_t hdr_histogram_value_at_percentile(
	const struct hdr_histogram* h, double percentile);


int hdr_histogram_init(
	int64_t lowest, int64_t highest, int sf, struct hdr_histogram** result)
{
	if (!result) return -1;
	struct hdr_histogram* h = (struct hdr_histogram*) calloc(
		1, sizeof(struct hdr_histogram));
	if (!h) return -1;
	h->lowest_trackable_value = lowest;
	h->highest_trackable_value = highest;
	h->significant_figures = sf;
	h->counts_len = 65536; // Simplified fixed size for benchmark
	h->counts = (int64_t*) calloc(h->counts_len, sizeof(int64_t));
	if (!h->counts) { free(h); return -1; }
	h->sub_bucket_count = 2048;
	h->bucket_count = h->counts_len / h->sub_bucket_count;
	*result = h;
	return 0;
}

void hdr_histogram_close(struct hdr_histogram* h)
{
	if (h) {
		free(h->counts);
		free(h);
	}
}

void hdr_histogram_reset(struct hdr_histogram* h)
{
	if (h) {
		memset(h->counts, 0, sizeof(int64_t) * h->counts_len);
		h->total_count = 0;
	}
}

static int32_t get_counts_index(struct hdr_histogram* h, int64_t value)
{
	if (value < h->lowest_trackable_value) value = h->lowest_trackable_value;
	if (value > h->highest_trackable_value) value = h->highest_trackable_value;
	if (value == 0) return 0;
	int64_t pof2 = (int64_t)floor(log2(value));
	int64_t bucket_index = pof2 > 11 ? pof2 - 11 : 0;
	int64_t sub_bucket_index = value >> bucket_index;
	int32_t index = (bucket_index * h->sub_bucket_count) +
		sub_bucket_index - h->sub_bucket_count;
	return index < h->counts_len ? index : h->counts_len - 1;
}

bool hdr_histogram_record_value(struct hdr_histogram* h, int64_t value)
{
	if (value < 0) return false;
	int32_t index = get_counts_index(h, value);
	if(index < 0 || index >= h->counts_len) return false;
	h->counts[index]++;
	h->total_count++;
	return true;
}

int64_t hdr_histogram_value_at_percentile(
	const struct hdr_histogram* h, double p)
{
	int64_t target_count = (int64_t)(((p / 100.0) * h->total_count) + 0.5);
	if (target_count == 0) return 0;
	int64_t current_count = 0;
	for (int i = 0; i < h->counts_len; i++) {
		current_count += h->counts[i];
		if (current_count >= target_count) {
			 int64_t bucket_index = i / h->sub_bucket_count;
			 int64_t sub_bucket_index = i % h->sub_bucket_count;
			 return (sub_bucket_index + h->sub_bucket_count) << bucket_index;
		}
	}
	return h->highest_trackable_value;
}

int64_t hdr_histogram_max(const struct hdr_histogram* h)
{
	for (int i = h->counts_len - 1; i >= 0; i--) {
		if (h->counts[i] > 0) {
			int64_t bucket_index = i / h->sub_bucket_count;
			int64_t sub_bucket_index = i % h->sub_bucket_count;
			return (sub_bucket_index + h->sub_bucket_count) << bucket_index;
		}
	}
	return 0;
}

double hdr_histogram_mean(const struct hdr_histogram* h)
{
	if (h->total_count == 0) return 0.0;
	int64_t total = 0;
	for (int i = 0; i < h->counts_len; i++) {
		if (h->counts[i] > 0) {
			int64_t bucket_index = i / h->sub_bucket_count;
			int64_t sub_bucket_index = i % h->sub_bucket_count;
			int64_t value =
				(sub_bucket_index + h->sub_bucket_count) << bucket_index;
			total += h->counts[i] * value;
		}
	}
	return (double)total / h->total_count;
}

#if defined(__cplusplus)
}
#endif

#endif // HDR_HISTOGRAM_H


