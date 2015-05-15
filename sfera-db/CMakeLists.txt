cmake_minimum_required(VERSION 2.8.4)
project(sfera-db)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/db_data_storage.cpp
    src/db_page.cpp
    src/database.hpp
    src/database.cpp
    src/test.cpp
    src/db_containers.cpp
    src/db_containers.hpp
    src/syscall_checker.hpp
    src/libsfera_db.cpp
    src/pages_cache.cpp
    src/pages_cache.hpp
    src/raw_file.cpp
    src/raw_file.hpp
    src/db_stable_storage_file.cpp
    src/db_stable_storage_file.hpp
    src/db_data_storage_config.hpp
    src/db_binlog_logger.cpp
    src/db_binlog_logger.hpp
    src/db_operation.cpp
    src/db_operation.h
    )

add_executable(sfera-db ${SOURCE_FILES})
