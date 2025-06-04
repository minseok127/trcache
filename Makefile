CC = gcc
AR = ar

CFLAGS_RELEASE = -Wall -Wextra -O2 -std=c11 -fPIC
CFLAGS_DEBUG = -Wall -Wextra -O0 -g -pg -std=c11 -fPIC -DTRCACHE_DEBUG

BUILD_MODE ?= release

ifeq ($(BUILD_MODE), release)
	CFLAGS = $(CFLAGS_RELEASE)
else ifeq ($(BUILD_MODE), debug)
	CFLAGS = $(CFLAGS_DEBUG)
else
	$(error Unknown BUILD_MODE: $(BUILD_MODE). Use 'release' or 'debug')
endif

PROJECT_ROOT := $(realpath .)
SRC_DIR := src
SUBDIRS := concurrent utils meta pipeline sched

OBJS = \
	$(wildcard $(SRC_DIR)/*/*.o)

INCLUDES = -I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include

STATIC_LIB = libtrcache.a
SHARED_LIB = libtrcache.so

.PHONY: all clean test

all:
	@for dir in $(SUBDIRS); do \
	$(MAKE) -C $(SRC_DIR)/$$dir CFLAGS="$(CFLAGS)" INCLUDES="$(INCLUDES)"; \
	done
	objs=$$(echo $(SRC_DIR)/*/*.o); \
	$(AR) rcs $(STATIC_LIB) $$objs; \
	$(CC) -shared -o $(SHARED_LIB) $$objs

clean:
	@for dir in $(SUBDIRS); do \
	$(MAKE) -C $(SRC_DIR)/$$dir clean; \
	done
	rm -f $(STATIC_LIB) $(SHARED_LIB)

test:
	$(MAKE) -C tests CFLAGS="$(CFLAGS)" INCLUDES="$(INCLUDES)" \
	PROJECT_ROOT="$(PROJECT_ROOT)" run
