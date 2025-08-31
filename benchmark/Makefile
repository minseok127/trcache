# Compiler and flags are intended to be inherited from the parent Makefile.
# Default values are provided for standalone execution.
CC ?= gcc
CFLAGS ?= -Wall -Wextra -O2 -std=c11 -g -march=native
PROJECT_ROOT := $(realpath ..)
TRCACHE_LIB := $(PROJECT_ROOT)/libtrcache.a

# Include paths for trcache headers
INCLUDES = -I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include

# Libraries to link
LDLIBS = -lpthread -lm

# Target executable
TARGET = benchmark

# Source files
SRCS = benchmark.c

OBJS = $(SRCS:.c=.o)

.PHONY: all clean run

all: $(TARGET)

# Link the benchmark executable against the trcache static library
$(TARGET): $(OBJS) $(TRCACHE_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TARGET) $(OBJS) $(TRCACHE_LIB) $(LDLIBS)

# Compile source files into object files
%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# A convenience target to run a default benchmark scenario
run: $(TARGET)
	@echo "Running a default HFT benchmark scenario..."
	./benchmark 8 1000 30s 10s 0 2 4

clean:
	rm -f $(TARGET) $(OBJS)

