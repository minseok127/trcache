CC = gcc

OBJS = hash_table.o hash_table_callbacks.o vector.o memstat.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
