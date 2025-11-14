CC = gcc
CFLAGS_RELEASE = -Wall -Wextra -O2 -std=c11 -fPIC -march=native
CFLAGS_DEBUG = -Wall -Wextra -O0 -g -std=c11 -fPIC -march=native

BUILD_MODE ?= release

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

# Target executables
TARGETS = feed_only_benchmark static_read_benchmark htap_benchmark

# Source files
FEED_ONLY_SRCS = feed_only_benchmark.c
STATIC_READ_SRCS = static_read_benchmark.c
HTAP_SRCS = htap_benchmark.c

FEED_ONLY_OBJS = $(FEED_ONLY_SRCS:.c=.o)
STATIC_READ_OBJS = $(STATIC_READ_SRCS:.c=.o)
HTAP_OBJS = $(HTAP_SRCS:.c=.o)

.PHONY: all clean

all: $(TARGETS)

# Feed-only benchmark
feed_only_benchmark: $(FEED_ONLY_OBJS) $(TRCACHE_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $(FEED_ONLY_OBJS) $(TRCACHE_LIB) $(LDLIBS)

# Static read benchmark
static_read_benchmark: $(STATIC_READ_OBJS) $(TRCACHE_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $(STATIC_READ_OBJS) $(TRCACHE_LIB) $(LDLIBS)

# HTAP benchmark
htap_benchmark: $(HTAP_OBJS) $(TRCACHE_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $(HTAP_OBJS) $(TRCACHE_LIB) $(LDLIBS)

# Compile source files into object files
%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(TARGETS) $(FEED_ONLY_OBJS) $(STATIC_READ_OBJS) $(HTAP_OBJS)
