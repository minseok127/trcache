CC = gcc

OBJS = sched_msg.o sched_pipeline_stats.o worker_state.o

.PHONY: all clean

all: $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f *.o
