//
// Created by Kiiro on 24-11-4.
//

#include "memtable.h"

#include <map>

using namespace std;

void Memtable::Put(const int64_t &key, const int64_t &value) {
    table[key] = value;
}

optional<int64_t> Memtable::Get(const int64_t &key) const {
    auto it = table.find(key);
    if (it != table.end()) {
        return it->second;
    }
    return nullopt;
}

vector<pair<int64_t, int64_t> > Memtable::Scan(const int64_t &startKey, const int64_t &endKey) const {
    vector<pair<int64_t, int64_t> > result;
    auto startIt = table.lower_bound(startKey);
    auto endIt = table.upper_bound(endKey);

    for (auto it = startIt; it != endIt; ++it) {
        result.emplace_back(it->first, it->second);
    }
    return result;
}
