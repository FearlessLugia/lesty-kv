cmake_minimum_required(VERSION 3.29)
project(kv)

set(CMAKE_CXX_STANDARD 20)

add_executable(kv-test
        tests/test_base.h
        tests/test_buffer_pool.cpp
        tests/test_runner.cpp
        tests/test_db.cpp)

add_library(kv src/main.cpp
        src/memtable.cpp
        src/memtable.h
        src/sstable.cpp
        src/sstable.h
        src/database.cpp
        src/database.h
        src/sstable.cpp
        src/buffer_pool.cpp
        src/buffer_pool.h
        utils/constants.h
        external/MurmurHash3.cpp
        external/MurmurHash3.h
)

target_link_libraries(kv-test PUBLIC kv)
