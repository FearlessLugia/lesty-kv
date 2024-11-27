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
#include "../utils/log.h"
#include "b_tree/b_tree_sstable.h"
#include "buffer_pool/buffer_pool_manager.h"
#include "sst_counter.h"

Database::Database(const size_t memtable_size) : memtable_(nullptr) {
    memtable_ = new Memtable(memtable_size);
    buffer_pool_ = BufferPoolManager::GetInstance();
}

Database::~Database() {
    {
        delete memtable_;
        // for (auto &sst: sstables_) {
        //     sst.~SSTable();
        // }
    }
}

void Database::Open(const string &db_name) {
    db_name_ = db_name;

    if (!filesystem::exists(db_name)) {
        filesystem::create_directory(db_name);
        LOG("Database created: " << db_name);
    } else {
        LOG("Database opened: " << db_name);
    }

    // Initialize SSTCounter with the database name, get current SST counter
    SSTCounter::GetInstance().Initialize(db_name);
}

vector<fs::path> Database::GetSortedSsts(const string &path) {
    vector<filesystem::path> ssts;
    regex filename_pattern;
    filename_pattern = R"(btree(\d+)\.bin)";

    for (const auto &entry: fs::directory_iterator(path)) {
        if (entry.is_regular_file() && regex_match(entry.path().filename().string(), filename_pattern)) {
            ssts.push_back(entry.path());
        }
    }

    ranges::sort(ssts, [](const fs::path &a, const fs::path &b) {
        const auto time_a = fs::last_write_time(a);
        const auto time_b = fs::last_write_time(b);

        return time_a > time_b;
    });

    return ssts;
}

void Database::Close() {
    if (memtable_->Size() > 0) {
        LOG("Closing database and flushing memtable to SSTs: " << db_name_);

        FlushToSst();
        memtable_->clear();
    }

    // for (auto &sst: sstables_) {
    //     sst.~SSTable();
    // }

    LOG("Database closed");
}

void Database::Put(const int64_t &key, const int64_t &value) {
    memtable_->Put(key, value);

    if (memtable_->Size() >= memtable_->memtable_size) {
        LOG(" Memtable is full, flushing to SSTs");
        FlushToSst();
        memtable_->clear();
    }
}

optional<int64_t> Database::Get(const int64_t &key) const {
    LOG("Get key: " << key);

    // find in memtable
    const auto value = memtable_->Get(key);
    if (value.has_value()) {
        return value;
    }

    // find in SSTs from the newest to the oldest
    auto ssts = GetSortedSsts(db_name_);

    for (const auto &sst_path: ssts) {
        LOG(" Get in " << sst_path.string());

        const auto sst = SSTable(sst_path);
        auto sst_value = sst.Get(key);
        if (sst_value.has_value()) {
            return sst_value;
        }
    }

    return nullopt;
}

vector<pair<int64_t, int64_t>> Database::Scan(const int64_t start_key, const int64_t end_key) const {
    LOG("Scan keys from " << start_key << " to " << end_key);

    vector<pair<int64_t, int64_t>> result;
    unordered_set<int64_t> found_keys;

    // find in memtable
    vector<pair<int64_t, int64_t>> memtable_results = memtable_->Scan(start_key, end_key);
    // Update result and found_keys
    for (const auto &[key, value]: memtable_results) {
        if (!found_keys.contains(key)) {
            result.emplace_back(key, value);
            found_keys.insert(key);
        }
    }

    // find in SSTs from the newest to the oldest
    auto ssts = GetSortedSsts(db_name_);
    // sstables_ = ssts;

    vector<pair<int64_t, int64_t>> values;
    for (const auto &sst_path: ssts) {
        LOG("\tScan in " << sst_path.string());

        const auto sst = SSTable(sst_path);
        const auto values = sst.Scan(start_key, end_key);
        // Update result and found_keys
        for (const auto &[key, value]: values) {
            if (!found_keys.contains(key)) {
                result.emplace_back(key, value);
                found_keys.insert(key);
            }
        }
    }

    ranges::sort(result, [](const auto &a, const auto &b) { return a.first < b.first; });

    return result;
}

void Database::FlushToSst() {
    FlushToBTreeSst();
    return;

    // Generate an increased-number-filename for the new SST file
    ostringstream file_name;
    // file_name << db_name_ << "/sst" << sst_counter_++ << ".bin";

    ofstream outfile(file_name.str(), ios::binary);
    if (!outfile) {
        cerr << "  File could not be opened" << endl;
        exit(1);
    }

    for (const auto &[key, value]: memtable_->TraversePair()) {
        outfile.write(reinterpret_cast<const char *>(&key), sizeof(key));
        outfile.write(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    // LOG("  Memtable flushed to SST: " << file_name.str());
    outfile.close();
}

void Database::FlushToBTreeSst() {
    // Generate an increased-number-filename for the new SST file
    const string db_name = SSTCounter::GetInstance().GetDbName();
    BTreeSSTable b_tree_sst = BTreeSSTable(db_name, true);

    const auto data = memtable_->Traverse();
    b_tree_sst.FlushFromMemtable(&data);

    // LOG("  Memtable flushed to SST: " << file_path);
}