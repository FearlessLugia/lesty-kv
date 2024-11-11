cmake_minimum_required(VERSION 3.29)
project(kv)

set(CMAKE_CXX_STANDARD 20)

add_executable(kv main.cpp
        memtable.cpp
        memtable.h
        database.cpp
        database.h
        constants.h
)
