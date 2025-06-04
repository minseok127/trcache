#define _GNU_SOURCE
#include <assert.h>
#include <string.h>
#include "meta/symbol_table.h"
#include "meta/trcache_internal.h"

static void *dummy_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
	(void)cache;
	(void)batch;
	(void)ctx;
	return NULL;
}

static bool dummy_is_done(trcache *cache, trcache_candle_batch *batch, void *handle) {
	(void)cache;
	(void)batch;
	(void)handle;
	return true;
}

static void dummy_destroy(void *handle, void *ctx) {
	(void)handle;
	(void)ctx;
}

int main(void) {
	struct trcache tc = {0};
	tc.candle_type_flags = 0;
	tc.num_candle_types = 0;
	tc.batch_candle_count_pow2 = 1;
	tc.batch_candle_count = 1 << tc.batch_candle_count_pow2;
	tc.flush_threshold_pow2 = 1;
	tc.flush_threshold = 1 << tc.flush_threshold_pow2;
	tc.flush_ops.flush = dummy_flush;
	tc.flush_ops.is_done = dummy_is_done;
	tc.flush_ops.destroy_handle = dummy_destroy;
	
	struct symbol_table *table = symbol_table_init(2);
	assert(table != NULL);
	
	int id1 = symbol_table_register(&tc, table, "AAA");
	assert(id1 == 0);
	int id2 = symbol_table_register(&tc, table, "BBB");
	assert(id2 == 1);
	int id3 = symbol_table_register(&tc, table, "CCC");
	assert(id3 == 2);
	
	assert(table->num_symbols == 3);
	assert(table->capacity >= 4);
	assert(symbol_table_lookup_symbol_id(table, "BBB") == 1);
	
	struct symbol_entry *e3 = symbol_table_lookup_entry(table, id3);
	assert(e3 != NULL);
	assert(strcmp(e3->symbol_str, "CCC") == 0);
	assert(e3->id == id3);
	
	assert(symbol_table_lookup_symbol_id(table, "DDD") == -1);
	assert(symbol_table_lookup_entry(table, 99) == NULL);
	
	symbol_table_destroy(table);
	return 0;
}
