//
// Created by Kiiro Huang on 24-11-10.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H


// int64_t occupies 8 Bytes
inline constexpr size_t kPairSize = 2 * sizeof(int64_t); // 16 Bytes

// let 1 page occupies 4KB
// 1 page contains 4 * 1024 / 16 = 256 key-value pairs
inline constexpr size_t kPageSize = 4096; // 4KB

// let 1 memtable contain 8 pages
inline constexpr size_t kPageNum = 8;

// 1 memtable contains 8 * 256 = 2048 key-value pairs
// 1 memtable contains 2048 * 16 = 32KB
inline constexpr size_t kMemtableSize = kPageSize * kPageNum; // 32KB


#endif //CONSTANTS_H
