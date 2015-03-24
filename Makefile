
SRC_FILES         := src/libmydb.cpp src/db_containers.cpp src/db_page.cpp src/db_file.cpp src/mydb.cpp
OUTPUT_PATH       := ./runner/libmydb.so
GXX_OPTIONS       := -std=c++11 
GXX_OPTIONS_DYLIB := -shared -fPIC


all: libmydb

libmydb:
	g++ $(SRC_FILES) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -O2 -DNDEBUG -o $(OUTPUT_PATH)

libmydb_dbg:
	g++ $(SRC_FILES) $(GXX_OPTIONS) $(GXX_OPTIONS_DYLIB) -g  -o $(OUTPUT_PATH)


sophia:
	make -C sophia/

clean:
	rm $(OUTPUT_PATH)
