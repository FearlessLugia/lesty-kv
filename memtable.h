//
// Created by Kiiro on 24-11-4.
//

#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <map>

using namespace std;

class Memtable {
public:
    void Put(const int64_t &key, const int64_t &value);

    optional<int64_t> Get(const int64_t &key) const;

    vector<pair<int64_t, int64_t>> Scan(const int64_t &startKey, const int64_t &endKey) const;

private:
    map<int64_t, int64_t> table;
};

#endif //MEMTABLE_H
