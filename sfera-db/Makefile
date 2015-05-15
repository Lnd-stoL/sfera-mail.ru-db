
#CPP_COMPILER      := clang++ -stdlib=libc++ #-fprofile-instr-use=../runner/code.profdata
CPP_COMPILER      := g++

LIB_FILES         := ../Hoard/src/libhoard.a
SRC_FILES         := src/*.cpp
OUTPUT_PATH       := ./bin/lib/libsfera_db.so

GXX_OPTIONS       := -std=c++11 -march=native
GXX_OPTIONS_DYLIB := -shared -fPIC


all: lib

lib:
	rm -f src/~all-src.cpp
	cat src/*.cpp > src/~all-src.cpp
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O3 -DNDEBUG -o $(OUTPUT_PATH) src/~all-src.cpp $(LIB_FILES)

lib_separated:
	rm -f src/~all-src.cpp
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O4 -DNDEBUG -o $(OUTPUT_PATH) $(SRC_FILES) $(LIB_FILES)


lib_dbg:
	rm -f src/~all-src.cpp
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g  -o $(OUTPUT_PATH) $(SRC_FILES) $(LIB_FILES)

lib_prof:
	rm -f src/~all-src.cpp
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -fprofile-instr-generate -g  -o $(OUTPUT_PATH) $(SRC_FILES) $(LIB_FILES)

clean:
	rm $(OUTPUT_PATH)
