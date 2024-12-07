CXX=g++
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
LDFLAGS= -lpthread
else
LDFLAGS= -lpthread -lrt
endif
CXXFLAGS= -O2 -std=c++17 -fno-builtin-memcmp
ifeq ($(shell uname -m), x86_64)
    CXXFLAGS += -msse -msse4.2
endif

.PHONY: clean libslash

all: conf_example cond_lock_example binlog_example mutex_example hash_example

CXXFLAGS+= -I../..

conf_example: libslash conf_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libslash.a $(LDFLAGS)

cond_lock_example: libslash cond_lock_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libslash.a $(LDFLAGS)

binlog_example: libslash binlog_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libslash.a $(LDFLAGS)

mutex_example: libslash mutex_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libslash.a $(LDFLAGS)

hash_example: libslash hash_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libslash.a $(LDFLAGS)

clean:
	find . -name "*.[oda]*" -exec rm -f {} \;
	rm -rf ./conf_example ./cond_lock_example ./binlog_example ./mutex_example ./hash_example

libslash:
	cd .. && $(MAKE) static_lib
