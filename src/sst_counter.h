//
// Created by Kiiro Huang on 2024-11-26.
//

#ifndef SST_COUNTER_H
#define SST_COUNTER_H
#include <cstdint>
#include <regex>

using namespace std;

class SSTCounter {
    // Level set to be 0, as it's the first level of the B-Tree
    vector<int64_t> level_counters_;
    string db_name_;

    SSTCounter() : level_counters_({}), db_name_("") {}

    SSTCounter(const SSTCounter &) = delete;
    SSTCounter &operator=(const SSTCounter &) = delete;

public:
    static SSTCounter &GetInstance();

    void Initialize(const string &db_name);

    string GetDbName() const;

    int64_t GetAndIncrementByLevel(int64_t level);
    string GenerateFileName(int64_t level);
};


#endif // SST_COUNTER_H
