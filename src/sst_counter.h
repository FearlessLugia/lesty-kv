//
// Created by Kiiro Huang on 2024-11-26.
//

#ifndef SST_COUNTER_H
#define SST_COUNTER_H
#include <cstdint>
#include <regex>

using namespace std;
namespace fs = std::filesystem;

class SSTCounter {
    int64_t counter_;
    string db_name_;

    SSTCounter() : counter_(0), db_name_("") {}

    SSTCounter(const SSTCounter &) = delete;
    SSTCounter &operator=(const SSTCounter &) = delete;

public:
    static SSTCounter &GetInstance() {
        static SSTCounter instance;
        return instance;
    }

    void Initialize(const string &db_name) {
        db_name_ = db_name;

        int64_t max_counter = -1;
        const regex filename_pattern(R"(btree(\d+)\.bin)");

        for (const auto &entry: fs::directory_iterator(db_name)) {
            string filename = entry.path().filename().string();
            smatch match;
            if (regex_match(filename, match, filename_pattern)) {
                int64_t file_counter = stoll(match[1].str());
                max_counter = max(max_counter, file_counter);
            }
        }

        counter_ = max_counter + 1;
    }

    string GetDbName() const { return db_name_; }

    int64_t GetCounter() const { return counter_; }

    int64_t GetAndIncrement() { return counter_++; }
};


#endif // SST_COUNTER_H
