/**
 * @file   candle_chunk.c
 * @brief  Implementation of row -> column staging chunk for trcache.
 *
 * This module manages one candle chunk, which buffers real‑time trades into
 * row‑form pages, converts them to column batches, and finally flushes the
 * result. All shared structures are protected by atomsnap gates to offer
 * lock‑free readers with minimal CAS on writers.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>

#include "core/candle_chunk.h"

#include "trcache.h"

/**
 * @brief   Allocate an atomsnap_version and its owned candle row page.
 *
 * The returned version already contains a zero‑initialized, 64‑byte‑aligned
 * row page in its @object field.
 *
 * @return  pointer to version, or NULL on failure.
 */
static struct atomsnap_version *row_page_version_alloc(
	void *unused __attribute__((unused)))
{
	struct atomsnap_version *version  = NULL;
	struct candle_row_page *row_page = NULL;

#if defined(_ISOC11_SOURCE) || (__STDC_VERSION__ >= 201112L)
	row_page = aligned_alloc(TRCACHE_SIMD_ALIGN,
		sizeof(struct candle_row_page));
#else
	posix_memalign((void **)&row_page, TRCACHE_SIMD_ALIGN,
			sizeof(struct candle_row_page));
#endif
	if (!row_page) {
		fprintf(stderr, "row_page_version_alloc: page alloc failed\n");
		return NULL;
	} else {
		memset(row_page, 0, sizeof(struct candle_row_page));
	}

	version = malloc(1, sizeof(struct atomsnap_version));
	if (version == NULL) {
		fprintf(stderr, "row_page_version_alloc: version alloc failed\n");
		free(row_page);
		return NULL;
	}

	version->object = row_page;
	return version;
}

static void row_page_version_free(struct atomsnap_version *version)
{
	if (version == NULL) {
        return;
	}

	free(version->object);
	free(version);
}

/**
 * @brief Allocate and initialize a candle chunk.
 *
 * @param seq_begin: first sequence number contained in this chunk (inclusive).
 *
 * The function sets up an atomsnap gate with one slot per row page.
 * Each slot lazily materializes its row page through row_page_version_alloc().
 *
 * @return Pointer to the new chunk, or NULL on failure.
 */
struct candle_chunk *candle_chunk_create(uint64_t seq_begin)
{
	struct candle_chunk *chunk = malloc(sizeof(struct candle_chunk));
    struct atomsnap_init_context ctx = {
		.atomsnap_alloc_impl = row_page_version_alloc,
		.atomsnap_free_impl = row_page_version_free,
		.num_extra_control_blocks = TRCACHE_NUM_ROW_PAGES - 1
	};

	if (chunk == NULL) {
		fprintf(stderr, "candle_chunk_create: chunk alloc failed\n");
		return NULL;
	}

	chunk->seq_begin = seq_begin;
	chunk->seq_end = seq_begin + TRCACHE_CHUNK_CAP;
	chunk->last_row_completed = seq_begin - 1;
	chunk->last_row_copied = seq_begin - 1;

	chunk->row_gate = atomsnap_init_gate(&ctx);
    if (chunk->row_gate == NULL) {
		fprintf(stderr, "candle_chunk_create: atomsnap_init failed\n");
		free(chunk);
		return NULL;
	}

	chunk->column_batch = trcache_batch_alloc_on_heap(TRCACHE_CHUNK_CAP);
	if (chunk->column_batch == NULL) {
		fprintf(stderr, "candle_chunk_create: batch alloc failed\n");
		atomsnap_destroy_gate(chunk->row_gate);
		free(chunk);
		return NULL;
	}

	return chunk;
}

/**
 * @brief Release all resources of a candle chunk.
 *
 * @param chunk: Candle-chunk pointer.
 */
void candle_chunk_destroy(struct candle_chunk *chunk)
{
	if (chunk == NULL) {
		return;
	}

	for (int i = 0; i < TRCACHE_NUM_ROW_PAGES; i++) {
		atomsnap_exchange_version_slot(chunk->row_gate, i, NULL);
	}

	atomsnap_destroy_gate(chunk->row_gate);
	trcache_batch_free(chunk->column_batch);
	free(chunk);
}

/**
 * @brief   Obtain (and possibly lazily allocate) the mutable row that must
 *          receive the next trade-data update.
 *
 * @param   chunk:         Candle-chunk pointer.
 * @param   handle [out]:  Output handle filled with mutable candle.
 *
 * Internal allocation of a fresh page (and its atomsnap gate registration) is
 * done transparently if the row lives on a not-yet-materialised page.
 */
void candle_chunk_get_mutable_row(struct candle_chunk *chunk,
	struct candle_chunk_mutate_handle *handle)
{
	uint64_t seq_next = chunk->last_row_completed + 1;
	int page_idx = candle_chunk_page_idx(chunk, seq_next);
	struct atomsnap_version *page_ver = NULL;
	struct candle_row_page *page = NULL;
	uint64_t page_first_seq

	if (handle == NULL) {
		fprintf(stderr, "candle_chunk_get_mutable_row: invalid handle\n");
		return;
	}

	/* chunk->seq_end is exclusive */
	if (seq_next >= chunk->seq_end) {
		handle->page_version = NULL;
		handle->row_ptr = NULL;
		handle->seq = -1;
		handle->page_slot = -1;
		handle->num_mutated = -1;
		return;
	}

	page_ver = atomsnap_acquire_version_slot(chunk->row_gate, page_idx);

	/*
	 * Page allocation being performed by a single thread is guaranteed by
	 * the admin thread.
	 */
	if (page_ver == NULL) {
		page_ver = atomsnap_make_version(chunk->row_gate, NULL);
		atomsnap_exchange_version_slot(chunk->row_gate, page_idx, page_ver);
		atomsnap_acquire_version_slot(chunk->row_gate, page_idx);
	}

	page = (struct candle_row_page *)page_ver->object;
}

/**
 * @brief   Declare that the row @seq_complete has met the "candle-completed"
 *          condition (OHLCV closed).
 *
 * @param   chunk:        Candle-chunk pointer.
 * @param   seq_complete: Sequence number of the finished row.
 *
 * Side-effect: updates the chunk’s @last_row_completed.
 */
void candle_chunk_mark_row_complete(struct candle_chunk *chunk,
	int seq_complete);

/**
 * @brief        Get the next contiguous range of *completed-but-uncopied*
