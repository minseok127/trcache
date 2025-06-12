CXX = g++
CXXFLAGS ?= -O2 -std=c++17 -I../../../ -I../../../src/include
LIBTRCACHE = ../../../libtrcache.a
LDFLAGS ?= $(LIBTRCACHE) -larrow -larrow_ipc

all: flush_example

flush_example: flush_example.o
	$(CXX) $(CXXFLAGS) flush_example.o $(LDFLAGS) -o $@

flush_example.o: flush_example.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f flush_example.o flush_example
