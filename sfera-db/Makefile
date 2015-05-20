
CLANG_COMPILER    := clang++-3.6 -stdlib=libc++
CPP_COMPILER      := g++

LIB_FILES         := ../Hoard/src/libhoard.a
SRC_FILES         := src/*.cpp
OUTPUT_PATH       := ./bin/lib/libsfera_db.so

GXX_OPTIONS       := -std=c++11 -march=native
GXX_OPTIONS_DYLIB := -shared -fPIC


_MERGED_SRC := .build-tmp/all-sources.cpp

all: lib_rel_clang_srcm

_src_merge:
	mkdir -p .build-tmp
	cp src/*.hpp .build-tmp	
	cp src/*.h .build-tmp	
	cat $(SRC_FILES) > $(_MERGED_SRC) 

lib_rel_clang_srcm: _src_merge
	$(CLANG_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O3 -DNDEBUG $(_MERGED_SRC) $(LIB_FILES) -o $(OUTPUT_PATH)

lib_rel_clang:
	$(CLANG_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O3 -flto -DNDEBUG $(SRC_FILES) $(LIB_FILES) -o $(OUTPUT_PATH)

lib_rel_gcc:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O3 -flto -DNDEBUG -o $(OUTPUT_PATH) $(SRC_FILES) $(LIB_FILES)

lib_rel_gcc_srcm: _src_merge
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O3 -DNDEBUG -o $(OUTPUT_PATH) $(_MERGED_SRC) $(LIB_FILES)


lib_gcc_dbg:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -o $(OUTPUT_PATH) $(SRC_FILES) $(LIB_FILES)

lib_clang_dbg:
	$(CLANG_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -o $(OUTPUT_PATH) $(SRC_FILES) $(LIB_FILES)


clean:
	rm $(OUTPUT_PATH)

