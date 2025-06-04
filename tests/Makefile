CC = gcc
PROJECT_ROOT ?= ..

TARGET = test_utils
OBJS = test_utils.o

INCLUDES ?= -I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include
SRC = $(PROJECT_ROOT)/src/utils/vector.c \
      $(PROJECT_ROOT)/src/utils/hash_table.c \
      $(PROJECT_ROOT)/src/utils/hash_table_callbacks.c

.PHONY: all run clean

all: $(TARGET)

$(TARGET): $(SRC) $(OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) $(SRC) $(OBJS) -o $@

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

run: all
	./$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
