CC = gcc

OBJS = trcache_internal.o symbol_table.o trade_data_buffer.o \
	trcache_candle_batch.o candle_chunk_list.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
