
CPP_COMPILER      := clang++ -stdlib=libc++ -fprofile-instr-use=../runner/code.profdata
#CPP_COMPILER      := g++

SRC_FILES         := src/*.cpp
OUTPUT_PATH       := ./bin/lib/libsfera_db.so

GXX_OPTIONS       := -std=c++11 -march=native
GXX_OPTIONS_DYLIB := -shared -fPIC


all: lib

lib:
	cat src/*.cpp > src/~all-src.cpp
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O4 -DNDEBUG -o $(OUTPUT_PATH) src/~all-src.cpp
	rm src/~all-src.cpp

lib_separated:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O4 -DNDEBUG -o $(OUTPUT_PATH) $(SRC_FILES)


lib_dbg:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g  -o $(OUTPUT_PATH) $(SRC_FILES)

lib_prof:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -fprofile-instr-generate -g  -o $(OUTPUT_PATH) $(SRC_FILES)

clean:
	rm $(OUTPUT_PATH)
