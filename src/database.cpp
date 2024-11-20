//
// Created by Kiiro Huang on 24-11-5.
//

#include "database.h"

#include <algorithm> // for std::sort
#include <fcntl.h>
#include <regex>
#include <sstream>
#include <unistd.h> // for pread, close
#include <unordered_set>

#include "../utils/constants.h"

void Database::Open(const string &db_name) {
    db_name_ = db_name;

    if (!filesystem::exists(db_name)) {
        filesystem::create_directory(db_name);
        cout << "Database created: " << db_name << endl;
        sst_counter_ = 0;
    } else {
        cout << "Database opened: " << db_name << endl;

        // find the largest SST file number
        sst_counter_ = 0;
        const regex filename_pattern(R"(sst_(\d+)\.bin)");
        for (const auto &entry: filesystem::directory_iterator(db_name)) {
            string filename = entry.path().filename().string();
            smatch match;
            if (regex_match(filename, match, filename_pattern)) {
                const long file_counter = stol(match[1].str());
                if (file_counter >= sst_counter_) {
                    sst_counter_ = file_counter + 1; // set the next number
                }
            }
        }
    }
}

vector<fs::path> Database::GetSortedSsts(const string &path) {
    vector<filesystem::path> ssts;
    const regex filename_pattern(R"(sst_(\d+)\.bin)");

    for (const auto &entry: fs::directory_iterator(path)) {
        if (entry.is_regular_file() && regex_match(entry.path().filename().string(), filename_pattern)) {
            ssts.push_back(entry.path());
        }
    }

    ranges::sort(ssts, [](const fs::path &a, const fs::path &b) {
        auto time_a = fs::last_write_time(a);
        auto time_b = fs::last_write_time(b);

        return time_a > time_b;
    });

    return ssts;
}

void Database::Close() {
    if (memtable_->Size() > 0) {
        cout << "Closing database and flushing memtable to SSTs: " << db_name_ << endl;

        FlushToSst();
        memtable_->clear();
    }

    // for (auto &sst: sstables_) {
    //     sst.~SSTable();
    // }

    cout << "Database closed" << endl;
}

void Database::Put(const int64_t &key, const int64_t &value) {
    memtable_->Put(key, value);

    if (memtable_->Size() >= memtable_->memtable_size) {
        cout << " Memtable is full, flushing to SSTs" << endl;
        FlushToSst();
        memtable_->clear();
    }
}

optional<int64_t> Database::Get(const int64_t &key) const {
    cout << "Get key: " << key << endl;

    // find in memtable
    const auto value = memtable_->Get(key);
    if (value.has_value()) {
        return value;
    }

    // find in SSTs from the newest to the oldest
    auto ssts = GetSortedSsts(db_name_);

    for (const auto &entry: ssts) {
        cout << " Get in " << entry.string() << endl;

        const auto sst = SSTable(entry);
        auto sst_value = sst.Get(key);
        if (sst_value.has_value()) {
            return sst_value;
        }
    }

    return nullopt;
}

vector<pair<int64_t, int64_t>> Database::Scan(const int64_t &startKey, const int64_t &endKey) const {
    cout << "Scan keys from " << startKey << " to " << endKey << endl;

    vector<pair<int64_t, int64_t>> result;
    unordered_set<int64_t> found_keys;

    // find in memtable
    vector<pair<int64_t, int64_t>> memtable_results = memtable_->Scan(startKey, endKey);
    result.insert(result.end(), memtable_results.begin(), memtable_results.end());

    // find in SSTs from the newest to the oldest
    auto ssts = GetSortedSsts(db_name_);

    vector<pair<int64_t, int64_t>> values;
    for (const auto &entry: ssts) {
        cout << "\tScan in " << entry.string() << endl;

        const auto sst = SSTable(entry);
        const auto values = sst.Scan(startKey, endKey);

        for (const auto &[key, value]: values) {
            if (!found_keys.contains(key)) {
                result.emplace_back(key, value);
                found_keys.insert(key);
            }
        }
    }

    sort(result.begin(), result.end(), [](const auto &a, const auto &b) { return a.first < b.first; });

    return result;
}

void Database::FlushToSst() {
    // Generate an increased-number-filename for the new SST file
    ostringstream filename;
    filename << db_name_ << "/sst_" << sst_counter_++ << ".bin";

    ofstream outfile(filename.str(), ios::binary);
    if (!outfile) {
        cerr << "  File could not be opened" << endl;
        exit(1);
    }

    for (const auto &[key, value]: memtable_->Traverse()) {
        outfile.write(reinterpret_cast<const char *>(&key), sizeof(key));
        outfile.write(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    cout << "  Memtable flushed to SST: " << filename.str() << endl;
    outfile.close();
}
