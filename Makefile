
all: libmydb


libmydb:
	g++ libmydb.cpp db_containers.cpp db_page.cpp db_file.cpp mydb.cpp -std=c++11 -O2 -shared -fPIC -o runner/libmydb.so


sophia:
	make -C sophia/

clean:
	rm libmydb.so
