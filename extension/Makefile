MODULE_big = paimon_heap

OBJS = \
    paimon_heap.o \
    paimon_ddl.o \
    paimon_shmem.o \
    paimon_ring.o \
    paimon_client.o \
    paimon_wal.o \
    paimon_bgworker.o \
    paimon_schema.o \
    paimon_parquet.o \
    paimon_avro.o \
    paimon_manifest.o \
    paimon_snapshot.o \
    paimon_table_writer.o \
    paimon_offload_s3.o \
    paimon_offload_hdfs.o

EXTENSION  = paimon_heap
DATA       = paimon_heap--1.0.sql

REGRESS      = paimon_heap
REGRESS_OPTS = --inputdir=test --load-extension=paimon_heap

AWS_SDK_ROOT     ?= /opt/aws-sdk-cpp
HDFS3_ROOT       ?= /usr/local
ARROW_INC        ?= /usr/include/arrow

# ── Normal (dynamic) link ────────────────────────────────────────────────────
SHLIB_LINK  = -lstdc++ -larrow -lparquet
SHLIB_LINK += -L$(AWS_SDK_ROOT)/lib -laws-cpp-sdk-s3 -laws-cpp-sdk-core
SHLIB_LINK += -Wl,-rpath,$(AWS_SDK_ROOT)/lib
SHLIB_LINK += -L$(HDFS3_ROOT)/lib -lhdfs3
SHLIB_LINK += -Wl,-rpath,$(HDFS3_ROOT)/lib
SHLIB_LINK += -lkrb5

PG_CPPFLAGS  = -I$(ARROW_INC) -I$(AWS_SDK_ROOT)/include -I$(HDFS3_ROOT)/include

PG_CONFIG   ?= /usr/local/pgsql/bin/pg_config
PGXS       := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Arrow 25 headers require C++20 (std::span, std::popcount, std::bit_width).
# PG_CXXFLAGS only reaches CXXFLAGS (used for .o); PGXS uses BITCODE_CXXFLAGS
# for LTO .bc targets via clang, so we must patch that variable too.
override BITCODE_CXXFLAGS += -std=c++20

# Single C++ compile rule for all .cpp files
%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++20 -fPIC -O2 -c -o $@ $<

paimon_client.o: paimon_client.cpp paimon_client.h paimon_shmem.h paimon_ring.h paimon_event.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++20 -fPIC -O2 -c -o $@ $<

# ── Fat (self-contained) build ───────────────────────────────────────────────
# All major C++ deps are statically linked; only libc/libstdc++/system TLS
# and compression libs remain as dynamic dependencies.
#
# Arrow/Parquet ship .a with PIC via the apt package.  Detect them via wildcard
# so the same rule works on amd64 (/usr/lib/x86_64-linux-gnu) and arm64.
# AWS_STATIC_ROOT should point to an AWS SDK built with BUILD_SHARED_LIBS=OFF.

ARROW_STATIC   ?= $(firstword $(wildcard /usr/lib/*/libarrow.a     /usr/local/lib/libarrow.a))
PARQUET_STATIC ?= $(firstword $(wildcard /usr/lib/*/libparquet.a   /usr/local/lib/libparquet.a))
ARROW_BUNDLED  ?= $(firstword $(wildcard /usr/lib/*/libarrow_bundled_dependencies.a))
HDFS3_STATIC   ?= $(HDFS3_ROOT)/lib/libhdfs3.a

.PHONY: fat
fat: $(OBJS)
	$(CXX) -shared -fPIC -o paimon_heap_fat.so $^ \
	  -Wl,--whole-archive \
	    $(PARQUET_STATIC) $(ARROW_STATIC) $(ARROW_BUNDLED) $(HDFS3_STATIC) \
	  -Wl,--no-whole-archive \
	  -lcurl -lssl -lcrypto -lz -lzstd -lbz2 -lsnappy \
	  -lprotobuf -lxml2 -lthrift -lkrb5 -lgssapi_krb5 -lsasl2 -lresolv -lstdc++
