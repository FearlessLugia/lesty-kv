//
// Created by Kiiro Huang on 24-11-10.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H

//------------ Page ------------

// int64_t occupies 8 Bytes
inline constexpr size_t kPairSize = 2 * sizeof(int64_t); // 16 Bytes

// let 1 page occupies 4KB
inline constexpr size_t kPageSize = 4096; // 4KB

// 1 page contains 4 * 1024 / 16 = 256 key-value pairs
inline constexpr size_t kPagePairs = kPageSize / kPairSize; // 256


//------------ Memtable ------------

// // let 1 memtable contain 8 pages
// inline constexpr size_t kPageNum = 8;
//
// // 1 memtable contains 8 * 256 = 2048 key-value pairs
// // 1 memtable contains 2048 * 16 = 32KB
// inline constexpr size_t kMemtableSize = kPageSize * kPageNum; // 32KB

// This parameter is for experiment
inline constexpr size_t kMemtableSize = 1024 * 1024; // 1MB

inline constexpr size_t kPageNum = kMemtableSize / kPageSize; // 256


//------------ Buffer Pool ------------

// 1 buffer pool occupies 32KB
// 1 buffer pool contains 32KB / 4KB = 8 pages
// inline constexpr size_t kBufferPoolSize = kMemtableSize; // 32KB

// This parameter is for experiment
inline constexpr size_t kBufferPoolSize = 10 * 1024 * 1024; // 10MB

// When buffer pool reaches 80% of its capacity, it will evict some pages
inline constexpr double kCoeffBufferPool = 0.8;

// When the key range of a scan covers over 0.25 * 8 = 2 pages, apply sequential flooding
// The pages covered during this scan will not be put into buffer pool
inline constexpr double kCoeffSequentialFlooding = 0.25;
inline constexpr double kPageSequentialFlooding = kCoeffSequentialFlooding * kPageNum;


//------------ B-Tree SSTable ------------

// Let B-Tree fan out be 1 page
inline constexpr size_t kFanOut = kPagePairs; // 256


//------------ LSM-Tree ------------

// The fixed size ratio between any two levels of LSM-Tree
inline constexpr size_t kLsmRatio = 3;

// Starting this level, apply Dostoevsky on LSM-Tree
// So, level 0 needs 3 (kLsmRatio) SSTables to merge
// Level 1 needs 9 SSTables to merge
// Level 2 needs 27 SSTables to merge
// ...
// Level kLevelToApplyDostoevsky needs 2 SSTables to merge
inline constexpr size_t kLevelToApplyDostoevsky = 4;


#endif // CONSTANTS_H
