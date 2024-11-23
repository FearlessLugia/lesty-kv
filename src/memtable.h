//
// Created by Kiiro on 24-11-4.
//

#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <fstream>
#include <iostream>
#include <map>

using namespace std;

class Memtable {
    map<int64_t, int64_t> table_;

public:
    size_t memtable_size;

    explicit Memtable(const size_t max_size) : memtable_size(max_size) {
    }

    void Put(const int64_t &key, const int64_t &value);

    optional<int64_t> Get(const int64_t &key) const;

    vector<pair<int64_t, int64_t> > Scan(const int64_t &startKey, const int64_t &endKey) const;

    vector<pair<int64_t, int64_t>> TraversePair() const;
    vector<int64_t> Traverse() const;

    void clear();

    size_t Size() const;
};

#endif //MEMTABLE_H
