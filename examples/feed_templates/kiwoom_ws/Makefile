CC = gcc
CFLAGS ?= -O2 -std=c11 -I../../../ -I../../../src/include
LIBTRCACHE = ../../../libtrcache.a
LDFLAGS ?= $(LIBTRCACHE) -lwebsockets -lcjson -pthread

all: feed_example

feed_example: feed_example.o
$(CC) $(CFLAGS) feed_example.o $(LDFLAGS) -o $@

feed_example.o: feed_example.c
$(CC) $(CFLAGS) -c $< -o $@

clean:
rm -f feed_example.o feed_example
