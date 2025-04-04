//
// Created by Kiiro Huang on 24-11-5.
//

#include "../include/database.h"

#include <__ranges/reverse_view.h>
#include <algorithm>
#include <fcntl.h>
#include <regex>
#include <sstream>
#include <unistd.h>
#include <unordered_set>

#include "../include/b_tree/b_tree_sstable.h"
#include "../include/buffer_pool/buffer_pool_manager.h"
#include "../include/lsm_tree/lsm_tree.h"
#include "../include/sst_counter.h"
#include "../utils/log.h"

Database::Database(const size_t memtable_size) : memtable_(nullptr) {
    memtable_ = new Memtable(memtable_size);
    buffer_pool_ = BufferPoolManager::GetInstance();
}

Database::~Database() {
    {
        delete memtable_;

        BufferPoolManager::GetInstance()->Clear();
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
    SSTCounter::GetInstance().SetDbName(db_name);

    // Build LSM-Tree
    LsmTree::GetInstance();
}

void Database::Close() const {
    if (memtable_->Size() > 0) {
        LOG("Closing database and flushing memtable to SSTs: " << db_name_);

        FlushFromMemtable();
        memtable_->clear();
    }

    BufferPoolManager::GetInstance()->Clear();

    LOG("Database closed");
    LOG("========================================");
}

void Database::Put(const int64_t key, const int64_t value) const {
    memtable_->Put(key, value);

    if (memtable_->Size() >= memtable_->memtable_size_) {
        LOG(" ┌Memtable is full, flushing to SST");
        FlushFromMemtable();
        memtable_->clear();
    }
}

optional<int64_t> Database::Get(const int64_t key) const {
    LOG("Get key: " << key);

    // Find in memtable
    const auto value = memtable_->Get(key);
    if (value.has_value()) {
        // If the value is INT64_MIN, it means the key is deleted
        if (value.value() == INT64_MIN) {
            return nullopt;
        }
        return value;
    }

    // Find in LSM-Tree
    const LsmTree &lsm_tree = LsmTree::GetInstance();

    // Find in SSTs from the lowest level to the highest level
    for (auto &current_level: lsm_tree.levelled_sst_) {
        // In the same level, find from the newest to the oldest
        for (const auto sst: ranges::reverse_view(current_level)) {
            sst->fd_ = sst->EnsureFileOpen();
            auto get_value = sst->Get(key);
            sst->fd_ = close(sst->fd_);

            if (get_value.has_value()) {
                // If the value is INT64_MIN, it means the key is deleted
                if (get_value.value() == INT64_MIN) {
                    return nullopt;
                }
                return get_value;
            }
        }
    }

    return nullopt;
}

vector<pair<int64_t, int64_t>> Database::Scan(const int64_t start_key, const int64_t end_key) const {
    LOG("Scan keys from " << start_key << " to " << end_key);

    vector<pair<int64_t, int64_t>> result;
    unordered_set<int64_t> found_keys;

    // Find in memtable
    vector<pair<int64_t, int64_t>> memtable_results = memtable_->Scan(start_key, end_key);
    // Update result and found_keys
    for (const auto &[key, value]: memtable_results) {
        // If the value is INT64_MIN, it means the key is deleted
        if (value == INT64_MIN) {
            continue;
        }

        if (!found_keys.contains(key)) {
            result.emplace_back(key, value);
            found_keys.insert(key);
        }
    }

    // Find in LSM-Tree
    const LsmTree &lsm_tree = LsmTree::GetInstance();

    // Find in SSTs from the lowest level to the highest level
    for (auto &current_level: lsm_tree.levelled_sst_) {
        // In the same level, find from the newest to the oldest
        for (const auto sst: ranges::reverse_view(current_level)) {
            LOG("\tScan in " << sst->file_path_);
            sst->fd_ = sst->EnsureFileOpen();
            const auto values = sst->Scan(start_key, end_key);
            sst->fd_ = close(sst->fd_);

            // Update result and found_keys
            for (const auto &[key, value]: values) {
                // If the value is INT64_MIN, it means the key is deleted
                if (value == INT64_MIN) {
                    continue;
                }

                if (!found_keys.contains(key)) {
                    result.emplace_back(key, value);
                    found_keys.insert(key);
                }
            }
        }
    }

    ranges::sort(result, [](const auto &a, const auto &b) { return a.first < b.first; });

    return result;
}

void Database::Delete(int64_t key) const {
    // Set tombstone in memtable
    // This is enough for the delete implementation
    memtable_->Delete(key);
}

void Database::FlushFromMemtable() const {
    // Flush to level 0 of LSM-Tree
    const string db_name = SSTCounter::GetInstance().GetDbName();
    const auto b_tree_sst = new BTreeSSTable(db_name, true);
    LOG(" | Flushing to SST: " << b_tree_sst->file_path_);

    // 1 memtable -> 1 SSTable
    const auto data = memtable_->Traverse();
    b_tree_sst->FlushToStorage(&data);

    LsmTree &lsm_tree = LsmTree::GetInstance();
    lsm_tree.AddSst(b_tree_sst);
    lsm_tree.OrderLsmTree();
}
