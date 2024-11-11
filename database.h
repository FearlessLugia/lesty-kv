//
// Created by Kiiro Huang on 24-11-5.
//

#ifndef DATABASE_H
#define DATABASE_H
#include <string>
#include <iostream>
#include <vector>

#include "memtable.h"

using namespace std;
namespace fs = std::filesystem;

class Database {
    string db_name_;
    Memtable memtable_;
    long sst_counter_;

    static size_t getFileSize(int fd);

    static optional<int64_t> binary_search_in_sst(const string &file_path, const int64_t &key);

    static vector<fs::path> get_sorted_ssts(const string &path);

    static vector<pair<int64_t, int64_t> > scan_in_sst(const string &file_path,
                                                       const int64_t &startKey, const int64_t &endKey);

public:
    explicit Database(const size_t memtable_size) : memtable_(memtable_size) {
    }

    void open(const string &db_name);

    void close();

    void put(const int64_t &key, const int64_t &value);

    optional<int64_t> get(const int64_t &key) const;

    vector<pair<int64_t, int64_t> > scan(const int64_t &startKey, const int64_t &endKey) const;

    void flush_to_sst();
};

#endif //DATABASE_H
