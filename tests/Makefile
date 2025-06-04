CC = gcc
PROJECT_ROOT ?= ..

INCLUDES ?= -I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include

TARGETS = test_utils test_symbol_table

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

.PHONY: all run clean

all: $(TARGETS)

test_utils: $(SRC_test_utils) $(OBJS_test_utils)
	$(CC) $(CFLAGS) $(INCLUDES) $(SRC_test_utils) $(OBJS_test_utils) -o $@

test_symbol_table: $(SRC_test_symbol_table) $(OBJS_test_symbol_table)
	$(CC) $(CFLAGS) $(INCLUDES) $(SRC_test_symbol_table) $(OBJS_test_symbol_table) -o $@

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

run: all
	./test_utils
	./test_symbol_table

clean:
	rm -f $(OBJS_test_utils) $(OBJS_test_symbol_table) $(TARGETS)
