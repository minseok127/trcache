CC = gcca

CFLAGS ?= -Wall -Wextra -O2 -std=c11 -fPIC

INCLUDES = -I../include -I../include/concurrent

OBJS = atomsnap.o scalable_queue.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
