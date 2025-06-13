CC = gcc -O0 -g
PROJECT_ROOT ?= ..

INCLUDES ?= -I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include

TARGETS = test_utils test_symbol_table \
          test_trade_data_buffer_no_freelist \
          test_trade_data_buffer_free_list \
          test_candle_chunk_list_multithread \
          test_trcache_api_multithread \
          test_trcache_api_single_worker \
          test_trcache_init_destroy

OBJS_test_utils = test_utils.o
SRC_test_utils = $(PROJECT_ROOT)/src/utils/vector.c \
      $(PROJECT_ROOT)/src/utils/hash_table.c \
      $(PROJECT_ROOT)/src/utils/hash_table_callbacks.c \

OBJS_test_symbol_table = test_symbol_table.o
SRC_test_symbol_table = $(PROJECT_ROOT)/src/utils/hash_table.c \
      $(PROJECT_ROOT)/src/utils/hash_table_callbacks.c \
      $(PROJECT_ROOT)/src/utils/vector.c \
      $(PROJECT_ROOT)/src/concurrent/atomsnap.c \
      $(PROJECT_ROOT)/src/pipeline/trade_data_buffer.c \
      $(PROJECT_ROOT)/src/pipeline/candle_chunk.c \
      $(PROJECT_ROOT)/src/pipeline/candle_chunk_list.c \
      $(PROJECT_ROOT)/src/pipeline/candle_chunk_index.c \
      $(PROJECT_ROOT)/src/pipeline/candle_update_ops.c \
      $(PROJECT_ROOT)/src/meta/symbol_table.c \
      $(PROJECT_ROOT)/src/meta/trcache_candle_batch.c

OBJS_test_trade_data_buffer_no_freelist = test_trade_data_buffer_no_freelist.o
SRC_test_trade_data_buffer_no_freelist = \
      $(PROJECT_ROOT)/src/pipeline/trade_data_buffer.c

OBJS_test_trade_data_buffer_free_list = test_trade_data_buffer_free_list.o
SRC_test_trade_data_buffer_free_list = \
      $(PROJECT_ROOT)/src/pipeline/trade_data_buffer.c

OBJS_test_candle_chunk_list_multithread = test_candle_chunk_list_multithread.o
SRC_test_candle_chunk_list_multithread = \
      $(PROJECT_ROOT)/src/pipeline/trade_data_buffer.c \
      $(PROJECT_ROOT)/src/pipeline/candle_chunk.c \
      $(PROJECT_ROOT)/src/pipeline/candle_chunk_list.c \
      $(PROJECT_ROOT)/src/pipeline/candle_chunk_index.c \
      $(PROJECT_ROOT)/src/pipeline/candle_update_ops.c \
      $(PROJECT_ROOT)/src/concurrent/atomsnap.c \
      $(PROJECT_ROOT)/src/meta/trcache_candle_batch.c

OBJS_test_trcache_api_multithread = test_trcache_api_multithread.o
SRC_test_trcache_api_multithread = $(wildcard $(PROJECT_ROOT)/src/*/*.c)

OBJS_test_trcache_api_single_worker = test_trcache_api_single_worker.o
SRC_test_trcache_api_single_worker = $(wildcard $(PROJECT_ROOT)/src/*/*.c)

OBJS_test_trcache_init_destroy = test_trcache_init_destroy.o
SRC_test_trcache_init_destroy = $(wildcard $(PROJECT_ROOT)/src/*/*.c)

.PHONY: all run clean

all: $(TARGETS)

test_utils: $(SRC_test_utils) $(OBJS_test_utils)
	$(CC) $(CFLAGS) $(INCLUDES) $(SRC_test_utils) $(OBJS_test_utils) -o $@

test_symbol_table: $(SRC_test_symbol_table) $(OBJS_test_symbol_table)
	$(CC) $(CFLAGS) $(INCLUDES) $(SRC_test_symbol_table) $(OBJS_test_symbol_table) -o $@

test_trade_data_buffer_no_freelist: $(SRC_test_trade_data_buffer_no_freelist) $(OBJS_test_trade_data_buffer_no_freelist)
	$(CC) $(CFLAGS) $(INCLUDES) -pthread $(SRC_test_trade_data_buffer_no_freelist) $(OBJS_test_trade_data_buffer_no_freelist) -o $@

test_trade_data_buffer_free_list: $(SRC_test_trade_data_buffer_free_list) $(OBJS_test_trade_data_buffer_free_list)
	$(CC) $(CFLAGS) $(INCLUDES) -pthread $(SRC_test_trade_data_buffer_free_list) $(OBJS_test_trade_data_buffer_free_list) -o $@

test_candle_chunk_list_multithread: $(SRC_test_candle_chunk_list_multithread) $(OBJS_test_candle_chunk_list_multithread)
	$(CC) $(CFLAGS) $(INCLUDES) -pthread $(SRC_test_candle_chunk_list_multithread) $(OBJS_test_candle_chunk_list_multithread) -o $@


test_trcache_api_multithread: $(SRC_test_trcache_api_multithread) $(OBJS_test_trcache_api_multithread)
	$(CC) $(CFLAGS) $(INCLUDES) -pthread $(SRC_test_trcache_api_multithread) $(OBJS_test_trcache_api_multithread) -o $@
test_trcache_api_single_worker: $(SRC_test_trcache_api_single_worker) $(OBJS_test_trcache_api_single_worker)
	$(CC) $(CFLAGS) $(INCLUDES) -pthread $(SRC_test_trcache_api_single_worker) $(OBJS_test_trcache_api_single_worker) -o $@

test_trcache_init_destroy: $(SRC_test_trcache_init_destroy) $(OBJS_test_trcache_init_destroy)
	$(CC) $(CFLAGS) $(INCLUDES) -pthread $(SRC_test_trcache_init_destroy) $(OBJS_test_trcache_init_destroy) -o $@

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

run: all
	./test_utils
	./test_symbol_table
	./test_trade_data_buffer_no_freelist
	./test_trade_data_buffer_free_list
	./test_candle_chunk_list_multithread
	./test_trcache_api_multithread
	./test_trcache_api_single_worker
	./test_trcache_init_destroy
	
clean:
	       rm -f $(OBJS_test_utils) $(OBJS_test_symbol_table) \
	       $(OBJS_test_trade_data_buffer_no_freelist) \
	               $(OBJS_test_trade_data_buffer_free_list) \
                       $(OBJS_test_candle_chunk_list_multithread) \
                       $(OBJS_test_trcache_api_multithread) \
                       $(OBJS_test_trcache_api_single_worker) \
                       $(OBJS_test_trcache_init_destroy) \
               $(TARGETS)
