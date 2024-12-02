//
// Created by Kiiro Huang on 2024-11-28.
//


#include "../include/sst_counter.h"

#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

SSTCounter &SSTCounter::GetInstance() {
    static SSTCounter instance;
    return instance;
}

string SSTCounter::GetDbName() const { return db_name_; }

void SSTCounter::SetDbName(const string &db_name) {
    db_name_ = db_name;
    level_counters_.clear();
}

void SSTCounter::SetLevelCounters(int64_t level, int64_t counter) {
    if (level >= level_counters_.size()) {
        level_counters_.resize(level + 1, 0);
    }
    level_counters_[level] = counter;
}

string SSTCounter::GenerateFileName(const int64_t level) {
    if (level >= level_counters_.size()) {
        level_counters_.resize(level + 1, 0);
    }
    const int64_t index = level_counters_[level]++;
    return "btree" + to_string(level) + "_" + to_string(index) + ".bin";
}
