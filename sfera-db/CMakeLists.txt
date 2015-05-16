cmake_minimum_required(VERSION 2.8.4)
project(sfera-db)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/db_data_storage.cpp
    src/db_page.cpp
    src/database.cpp
    src/test.cpp
    src/db_containers.cpp
    src/libsfera_db.cpp
    src/pages_cache.cpp
    src/raw_file.cpp
    src/db_stable_storage_file.cpp
    src/db_binlog_logger.cpp
    src/db_operation.cpp

    src/syscall_checker.hpp
    src/db_data_storage_config.hpp
    )

add_executable(sfera-db ${SOURCE_FILES})
