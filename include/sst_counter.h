//
// Created by Kiiro Huang on 2024-11-26.
//

#ifndef SST_COUNTER_H
#define SST_COUNTER_H
#include <cstdint>
#include <regex>

using namespace std;

class SSTCounter {
    vector<int64_t> level_counters_;
    string db_name_;

    SSTCounter() : level_counters_({}){}

    SSTCounter(const SSTCounter &) = delete;
    SSTCounter &operator=(const SSTCounter &) = delete;

public:
    static SSTCounter &GetInstance();

    void SetDbName(const string &db_name);

    void SetLevelCounters(int64_t level, int64_t counter);

    [[nodiscard]] string GetDbName() const;

    string GenerateFileName(int64_t level);
};


#endif // SST_COUNTER_H
