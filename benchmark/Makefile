CC = gcc
CFLAGS_RELEASE = -Wall -Wextra -O2 -std=c11 -fPIC -march=native
CFLAGS_DEBUG = -Wall -Wextra -O0 -g -std=c11 -fPIC -march=native

BUILD_MODE ?= debug

ifeq ($(BUILD_MODE), release)
	CFLAGS = $(CFLAGS_RELEASE)
else ifeq ($(BUILD_MODE), debug)
	CFLAGS = $(CFLAGS_DEBUG)
else
	$(error Unknown BUILD_MODE: $(BUILD_MODE). Use 'release' or 'debug')
endif

PROJECT_ROOT := $(realpath ..)
TRCACHE_LIB := $(PROJECT_ROOT)/libtrcache.a

# Include paths for trcache headers
INCLUDES = -I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include

# Libraries to link
LDLIBS = -lpthread -lm

# Target executable
TARGET = feed_only_benchmark

# Source files
SRCS = feed_only_benchmark.c

OBJS = $(SRCS:.c=.o)

.PHONY: all clean

all: $(TARGET)

# Link the benchmark executable against the trcache static library
$(TARGET): $(OBJS) $(TRCACHE_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TARGET) $(OBJS) $(TRCACHE_LIB) $(LDLIBS)

# Compile source files into object files
%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJS)

