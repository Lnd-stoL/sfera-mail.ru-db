
CPP_COMPILER      := clang++ -stdlib=libc++ -fprofile-instr-use=../runner/code.profdata
#CPP_COMPILER      := g++

SRC_FILES         := src/libsfera_db.cpp src/pages_cache.cpp src/db_containers.cpp src/db_page.cpp src/db_file_storage.cpp src/database.cpp
OUTPUT_PATH       := ./bin/lib/libsfera_db.so

GXX_OPTIONS       := -std=c++11 -march=native
GXX_OPTIONS_DYLIB := -shared -fPIC


all: lib

lib:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g -O3 -DNDEBUG -o $(OUTPUT_PATH) $(SRC_FILES)

lib_dbg:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g  -o $(OUTPUT_PATH) $(SRC_FILES)

lib_prof:
	$(CPP_COMPILER) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -fprofile-instr-generate -g  -o $(OUTPUT_PATH) $(SRC_FILES)

clean:
	rm $(OUTPUT_PATH)
