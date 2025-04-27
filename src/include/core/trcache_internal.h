#ifndef TRCACHE_INTERNAL_H
#define TRCACHE_INTERNAL_H

struct trcache_symbol_entry {
	pthread_spinlock_t spinlock;
	int num_worker_thread;
};

struct trcache {
	struct trcache_symbol_entry symbol_entry_table[TRCACHE_MAX_SYMBOL_COUNT];
	int num_symbol_entry;
	int trcache_id;
};

#endif /* TRCACHE_INTERNAL_H */
