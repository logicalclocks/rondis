CXX=g++
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
LDFLAGS= -lpthread
else
LDFLAGS= -lpthread -lrt -lprotobuf
endif
CXXFLAGS=-O2 -std=c++17 -fno-builtin-memcmp
ifeq ($(shell uname -m), x86_64)
    CXXFLAGS += -msse -msse4.2
endif

.PHONY: clean all

all: rondis

PINK_PATH=$(CURDIR)/../..
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
	SLASH_PATH=$(CURDIR)/../third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

ifndef RONDB_PATH
  $(warning Warning: missing rondb path)
  $(error Exit due to missing RONDB_PATH)
endif
RONDB_INCLUDE_DIR=$(RONDB_PATH)/include/storage/ndb
ifeq ($(UNAME_S),Darwin)
RONDB_LIBRARY=$(RONDB_PATH)/lib/libndbclient.dylib
else
RONDB_LIBRARY=$(RONDB_PATH)/lib/libndbclient.so
endif

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(RONDB_INCLUDE_DIR)

DEP_LIBS = $(PINK_LIBRARY) $(SLASH_LIBRARY) $(RONDB_LIBRARY)
ifeq ($(UNAME_S),Darwin)
DEP_LIBS+= /opt/homebrew/lib/libprotobuf.dylib
endif
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

# Use find to locate all .cc files in subdirectories
SOURCES = $(CURDIR)/rondis.cc $(CURDIR)/rondb.cc $(CURDIR)/common.cc $(CURDIR)/string/table_definitions.cc $(CURDIR)/string/commands.cc $(CURDIR)/string/db_operations.cc $(CURDIR)/string/interpreted_code.cc
OBJECTS = $(SOURCES:.cc=.o)

# Target to build the executable "rondis"
rondis: $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJECTS) $(LDFLAGS)

# Clean target to remove generated files
clean:
	rm -f $(OBJECTS) rondis
