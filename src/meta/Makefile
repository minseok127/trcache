CC = gcc

OBJS = trcache_internal.o symbol_table.o trcache_candle_batch.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
