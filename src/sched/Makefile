CC = gcc

OBJS = sched_msg.o sched_pipeline_stats.o admin_thread.o worker_thread.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
