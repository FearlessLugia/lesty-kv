//
// Created by Kiiro Huang on 2024-11-26.
//

#ifndef SST_COUNTER_H
#define SST_COUNTER_H
#include <cstdint>
#include <regex>
#include <string>
#include <vector>

class SSTCounter {
    std::vector<int64_t> level_counters_;
    std::string db_name_;

public:
    SSTCounter() : level_counters_({}){}

    void SetDbName(const std::string &db_name);

    void SetLevelCounters(int64_t level, int64_t counter);

    [[nodiscard]] std::string GetDbName() const;

    std::string GenerateFileName(int64_t level);
};


#endif // SST_COUNTER_H
