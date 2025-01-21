cmake_minimum_required(VERSION 3.29)
project(kv)

set(CMAKE_CXX_STANDARD 20)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif ()

add_library(kv-lib
        include/database.h
        include/memtable.h
        include/sstable.h
        include/sst_counter.h
        include/buffer_pool/Page.h
        include/buffer_pool/bucket_node.h
        include/buffer_pool/buffer_pool.h
        include/buffer_pool/eviction_policy.h
        include/buffer_pool/lru/queue_node.h
        include/buffer_pool/lru/lru.h
        include/buffer_pool/buffer_pool_manager.h
        include/b_tree/b_tree_sstable.h
        include/lsm_tree/lsm_tree.h
        src/memtable.cpp
        src/sstable.cpp
        src/database.cpp
        src/buffer_pool/buffer_pool.cpp
        src/buffer_pool/lru/lru.cpp
        src/b_tree/b_tree_sstable.cpp
        src/lsm_tree/lsm_tree.cpp
        src/sst_counter.cpp
        utils/constants.h
        utils/log.h
        external/MurmurHash3.cpp
        external/MurmurHash3.h
)

add_executable(kv-test
        tests/test_base.h
        tests/test_runner.cpp
        tests/test_db.cpp
        tests/test_buffer_pool.cpp
        tests/test_b_tree.cpp
        tests/test_lsm_tree.cpp)

add_executable(kv-experiment
        experiments/experiment.cpp
)

target_link_libraries(kv-test PUBLIC kv-lib)
target_link_libraries(kv-experiment PUBLIC kv-lib)

target_compile_options(kv-test PRIVATE
        $<$<CONFIG:Debug>:-g> # Debug mode
        $<$<CONFIG:Release>:-DNDEBUG> # Release mode - No logs
)