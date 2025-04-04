//
// Created by Kiiro on 24-11-4.
//

#include "../include/memtable.h"

using namespace std;

void Memtable::Put(const int64_t key, const int64_t value) { table_[key] = value; }

optional<int64_t> Memtable::Get(const int64_t key) const {
    const auto it = table_.find(key);
    if (it != table_.end()) {
        return it->second;
    }
    return nullopt;
}

vector<pair<int64_t, int64_t>> Memtable::Scan(const int64_t startKey, const int64_t endKey) const {
    vector<pair<int64_t, int64_t>> result;
    const auto start_it = table_.lower_bound(startKey);
    const auto end_it = table_.upper_bound(endKey);

    for (auto it = start_it; it != end_it; ++it) {
        result.emplace_back(it->first, it->second);
    }
    return result;
}

void Memtable::Delete(const int64_t key) { Put(key, INT64_MIN); }

vector<int64_t> Memtable::Traverse() const {
    vector<int64_t> result;

    for (const auto &[fst, snd]: table_) {
        result.push_back(fst);
        result.push_back(snd);
    }
    return result;
}

void Memtable::clear() { table_.clear(); }

size_t Memtable::Size() const { return table_.size() * sizeof(int64_t) * 2; }
