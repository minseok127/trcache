CC = gcc

OBJS = trcache_internal.o symbol_table.o trade_data_buffer.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
