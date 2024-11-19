//
// Created by Kiiro Huang on 24-11-5.
//

#ifndef DATABASE_H
#define DATABASE_H
#include <string>
#include <iostream>
#include <vector>

#include "memtable.h"
#include "sstable.h"

using namespace std;
namespace fs = std::filesystem;

class Database {
    string db_name_;
    Memtable memtable_;

    long sst_counter_;
    vector<SSTable> sstables_;

    // static off_t get_file_size(int fd);

    // static optional<int64_t> binary_search_in_sst(const string &file_path, const int64_t &key);

    static vector<fs::path> GetSortedSsts(const string &path);

    // static vector<pair<int64_t, int64_t> > scan_in_sst(const string &file_path,
                                                       // const int64_t &startKey, const int64_t &endKey);

public:
    explicit Database(const size_t memtable_size) : memtable_(memtable_size) {
    }

    void Open(const string &db_name);

    void Close();

    void Put(const int64_t &key, const int64_t &value);

    optional<int64_t> Get(const int64_t &key) const;

    vector<pair<int64_t, int64_t> > Scan(const int64_t &startKey, const int64_t &endKey) const;

    void FlushToSst();
};

#endif //DATABASE_H
