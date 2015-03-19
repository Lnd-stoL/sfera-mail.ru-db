cmake_minimum_required(VERSION 2.8.4)
project(sfera-mydbms)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    db_file.cpp
    db_page.cpp
    mydb.hpp
    mydb.cpp
    test.cpp
    db_containers.cpp
    db_containers.hpp
    syscall_checker.h
    libmydb.cpp
    )

add_executable(Technosphere_spring2015_DB ${SOURCE_FILES})
