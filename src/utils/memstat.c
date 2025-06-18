#include "utils/memstat.h"
#include "utils/log.h"

struct memstat g_memstat;

static const char *cat_name[MEMSTAT_CATEGORY_NUM] = {
[MEMSTAT_TRADE_DATA_BUFFER] = "trade_data_buffer",
[MEMSTAT_CANDLE_CHUNK_LIST] = "candle_chunk_list",
[MEMSTAT_CANDLE_CHUNK_INDEX] = "candle_chunk_index",
[MEMSTAT_SCQ_NODE] = "scq_node",
[MEMSTAT_SCHED_MSG] = "sched_msg",
};

void memstat_errmsg_status(void)
{
size_t total = 0;
for (int i = 0; i < MEMSTAT_CATEGORY_NUM; ++i) {
size_t val = memstat_get((memstat_category)i);
total += val;
errmsg(stderr, "%s: %zu bytes\n", cat_name[i], val);
}
errmsg(stderr, "total: %zu bytes\n", total);
}
