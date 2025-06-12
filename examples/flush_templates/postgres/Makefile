CC = gcc
CFLAGS ?= -O2 -std=c11 -I../../../ -I../../../src/include
LIBTRCACHE = ../../../libtrcache.a
LDFLAGS ?= $(LIBTRCACHE) -lpq

all: flush_example

flush_example: flush_example.o
	$(CC) $(CFLAGS) flush_example.o $(LDFLAGS) -o $@

flush_example.o: flush_example.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f flush_example.o flush_example
