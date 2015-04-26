
CPP_COMPILER      := clang++ -stdlib=libc++

SRC_FILES         := src/libsfera_db.cpp src/db_containers.cpp src/db_page.cpp src/db_file_storage.cpp src/database.cpp
OUTPUT_PATH       := ../runner/libmydb.so
GXX_OPTIONS       := -std=c++11 -march=native
GXX_OPTIONS_DYLIB := -shared -fPIC


all: libmydb

libmydb:
	$(CPP_COMPILER) $(SRC_FILES) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -O3 -DNDEBUG -o $(OUTPUT_PATH)

libmydb_dbg:
	$(CPP_COMPILER) $(SRC_FILES) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g  -o $(OUTPUT_PATH)

clean:
	rm $(OUTPUT_PATH)
