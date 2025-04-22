CC = gcc
AR = ar

CFLAGS_RELEASE = -Wall -Wextra -O2 -std=c11 -fPIC
CFLAGS_DEBUG = -Wall -Wextra -O0 -g -pg -std=c11 -fPIC

BUILD_MODE ?= release

ifeq ($(BUILD_MODE), release)
	CFLAGS = $(CFLAGS_RELEASE)
else ifeq ($(BUILD_MODE), debug)
	CFLAGS = $(CFLAGS_DEBUG)
else
	$(error Unknown BUILD_MODE: $(BUILD_MODE). Use 'release' or 'debug')
endif

SRC_DIR := src
SUBDIRS := concurrent

OBJS := $(foreach dir, $(SUBDIRS), $(wildcard $(SRC_DIR)/$(dir)/*.o))

STATIC_LIB = libtrcache.a
SHARED_LIB = libtrcache.so

.PHONY: all clean

all:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $(SRC_DIR)/$$dir CFLAGS="$(CFLAGS)"; \
	done
	$(AR) src $(STATIC_LIB) $(OBJS)
	$(CC) -shared -o $(SHARED_LIB) $(OBJS)

clean:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $(SRC_DIR)/$$dir clean; \
	done
	rm -f $(STATIC_LIB) $(SHARED_LIB)
