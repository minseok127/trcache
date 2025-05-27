CC = gcc

OBJS = trade_data_buffer.o candle_chunk.o candle_chunk_list.o \
	candle_chunk_index.o candle_update_ops.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
