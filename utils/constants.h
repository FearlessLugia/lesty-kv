//
// Created by Kiiro Huang on 24-11-10.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H


// int64_t occupies 8 Bytes
inline constexpr size_t kPairSize = 2 * sizeof(int64_t); // 16 Bytes

// let 1 page occupies 4KB
inline constexpr size_t kPageSize = 4096; // 4KB

// 1 page contains 4 * 1024 / 16 = 256 key-value pairs
inline constexpr size_t kPagePairs = kPageSize / kPairSize; // 256


// let 1 memtable contain 8 pages
inline constexpr size_t kPageNum = 8;

// 1 memtable contains 8 * 256 = 2048 key-value pairs
// 1 memtable contains 2048 * 16 = 32KB
inline constexpr size_t kMemtableSize = kPageSize * kPageNum; // 32KB

// 1 buffer pool occupies 32KB
// 1 buffer pool contains 32KB / 4KB = 8 pages
inline constexpr size_t kBufferPoolSize = kMemtableSize; // 32KB

// This parameter is for experiment
// 1 buffer pool occupies 1MB
// 1 buffer pool contains 1024 / 4 = 256 pages
// inline constexpr size_t kBufferPoolSize = 1024 * 1024; // 1MB

// when buffer pool reaches 80% of its capacity, it will evict some pages
inline constexpr double kCoeffBufferPool = 0.8;

// when the key range of a scan covers over 0.25 * 8 = 2 pages, apply sequential flooding
// The pages covered during this scan will not be put into buffer pool
inline constexpr double kCoeffSequentialFlooding = 0.25;
inline constexpr double kPageSequentialFlooding = kCoeffSequentialFlooding * kPageNum;

#endif // CONSTANTS_H
