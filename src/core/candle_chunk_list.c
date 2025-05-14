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

/**
 * @brief   Final cleanup for a row page version.
 *
 * @param   version: Pointer to the atomsnap_version
 *
 * Called by the last thread to release its reference to the version.
 */
static void row_page_version_free(struct atomsnap_version *version)
{
	if (version == NULL) {
        return;
	}

	free(version->object); /* #candle_row_page */
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
static struct candle_chunk *candle_chunk_create(uint64_t seq_begin)
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
static void candle_chunk_destroy(struct candle_chunk *chunk)
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
