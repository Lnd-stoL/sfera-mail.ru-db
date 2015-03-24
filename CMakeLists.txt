cmake_minimum_required(VERSION 2.8.4)
project(sfera-mydbms)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/db_file.cpp
    src/db_page.cpp
    src/mydb.hpp
    src/mydb.cpp
    src/test.cpp
    src/db_containers.cpp
    src/db_containers.hpp
    src/syscall_checker.h
    src/libmydb.cpp
    )

add_executable(Technosphere_spring2015_DB ${SOURCE_FILES})
