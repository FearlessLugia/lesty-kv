cmake_minimum_required(VERSION 3.29)
project(kv)

set(CMAKE_CXX_STANDARD 20)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif ()

add_executable(kv src/main.cpp)

add_library(kv-lib src/memtable.cpp
        src/memtable.h
        src/sstable.cpp
        src/sstable.h
        src/database.cpp
        src/database.h
        utils/constants.h
        utils/log.h
        external/MurmurHash3.cpp
        external/MurmurHash3.h
        src/buffer_pool/Page.h
        src/buffer_pool/bucket_node.h
        src/buffer_pool/buffer_pool.cpp
        src/buffer_pool/buffer_pool.h
        src/buffer_pool/buffer_pool_manager.h
        src/buffer_pool/directory.cpp
        src/buffer_pool/directory.h
        src/buffer_pool/lru/queue_node.h
        src/buffer_pool/lru/eviction_policy.h
        src/buffer_pool/lru/lru.cpp
        src/buffer_pool/lru/lru.h
        src/b_tree/b_tree_sstable.cpp
        src/b_tree/b_tree_sstable.h
)

add_executable(kv-test
        tests/test_base.h
        tests/test_runner.cpp
        tests/test_db.cpp
        tests/test_buffer_pool.cpp
        tests/test_b_tree.cpp)


target_link_libraries(kv PUBLIC kv-lib)
target_link_libraries(kv-test PUBLIC kv-lib)

target_compile_options(kv-test PRIVATE
        $<$<CONFIG:Debug>:-g> # Debug mode
        $<$<CONFIG:Release>:-DNDEBUG> # Release mode - No logs
)