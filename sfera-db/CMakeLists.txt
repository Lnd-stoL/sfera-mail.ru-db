cmake_minimum_required(VERSION 2.8.4)
project(sfera-db)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/db_file_storage.cpp
    src/db_page.cpp
    src/database.hpp
    src/database.cpp
    src/test.cpp
    src/db_containers.cpp
    src/db_containers.hpp
    src/syscall_checker.h
    src/libsfera_db.cpp
    )

add_executable(sfera-db ${SOURCE_FILES} src/pages_cache.cpp src/pages_cache.h)
