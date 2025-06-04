#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include "utils/vector.h"
#include "utils/hash_table.h"
#include "utils/hash_table_callbacks.h"

int main(void) {
	/* vector tests */
	struct vector *vec = vector_init(sizeof(int));
	assert(vec != NULL);

	for (int i = 0; i < 3; i++) {
	    assert(vector_push_back(vec, &i) == 0);
	}
	assert(vector_size(vec) == 3);
	int *v1 = vector_at(vec, 1);
	assert(*v1 == 1);

	vector_pop_back(vec);
	assert(vector_size(vec) == 2);
	vector_clear(vec);
	assert(vector_size(vec) == 0);
	vector_destroy(vec);

	/* hash table tests */
	ht_hash_table *table = ht_create(8, 0,
		murmur_hash,
		compare_symbol_str,
		duplicate_symbol_str,
		free_symbol_str);
	assert(table != NULL);

	int val1 = 42;
	int val2 = 84;
	assert(ht_insert(table, "key", 4, &val1) == 0);
	assert(ht_insert(table, "key2", 5, &val2) == 0);

	bool found = false;
	int *out = ht_find(table, "key", 4, &found);
	assert(found && out && *out == 42);

	out = ht_find(table, "key2", 5, &found);
	assert(found && out && *out == 84);

	ht_remove(table, "key", 4);
	out = ht_find(table, "key", 4, &found);
	assert(!found && out == NULL);

	ht_destroy(table);

	return 0;
}
