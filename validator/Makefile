# Validator Build Script
#
# Dependencies:
# - trcache (Local or Installed)
# - nlohmann/json (Header only)
# - openssl
# - libcurl
#

# Output Binary
TARGET = validator_app

# Compiler Settings
CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread
CXXFLAGS += -I./core -I./exchanges

# Dependency Paths (Adjust as needed)
# Corrected path to point to the project root (where trcache.h resides)
TRCACHE_ROOT = ..
CXXFLAGS += -I$(TRCACHE_ROOT) -I$(TRCACHE_ROOT)/src/include

# Libraries
# Added -Wl,-rpath=$(TRCACHE_ROOT) to embed library path into the binary
LDFLAGS = -L$(TRCACHE_ROOT) -Wl,-rpath=$(TRCACHE_ROOT) -ltrcache -lssl -lcrypto -lcurl

# Source Files
SRCS = main.cpp \
       core/config_loader.cpp \
       core/engine.cpp \
       core/auditor.cpp \
       exchanges/binance/binance_client.cpp

# Object Files
OBJS = $(SRCS:.cpp=.o)

# Build Rules
all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
