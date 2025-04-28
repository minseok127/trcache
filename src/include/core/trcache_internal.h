#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

#include <stddef.h>
#include <pthread.h>

#include "core/symbol_table.h"

#include "trcache.h"

struct trcache {
	int trcache_id;
	int num_workers;
	trcache_candle_type_flags candle_type_flags;
	struct symbol_table *symbol_table;
};

#endif /* TRCACHE_INTERNAL_H */
