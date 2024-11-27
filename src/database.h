//
// Created by Kiiro Huang on 24-11-5.
//

#ifndef DATABASE_H
#define DATABASE_H
#include <iostream>
#include <string>
#include <vector>

#include "buffer_pool/buffer_pool.h"
#include "memtable.h"
#include "sstable.h"

using namespace std;
namespace fs = std::filesystem;

class Database {
    string db_name_;
    Memtable *memtable_;
    BufferPool *buffer_pool_;

    // vector<SSTable> *sstables_;

    vector<fs::path> sstables_;

    static vector<fs::path> GetSortedSsts(const string &path);

public:
    explicit Database(const size_t memtable_size);

    ~Database();

    void Open(const string &db_name);

    void Close();

    void Put(const int64_t &key, const int64_t &value);

    optional<int64_t> Get(const int64_t &key) const;

    vector<pair<int64_t, int64_t>> Scan(int64_t start_key, int64_t end_key) const;

    void FlushToSst();

    void FlushToBTreeSst();
};

#endif // DATABASE_H
