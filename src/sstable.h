//
// Created by Kiiro on 24-11-4.
//

#ifndef SSTABLE_H
#define SSTABLE_H
#include <fstream>

#include "buffer_pool/buffer_pool.h"
#include "buffer_pool/page.h"

using namespace std;
class SSTable {
public:
    string file_path_;
    int fd_;
    off_t file_size_;

    vector<Page *> pages_;

    int64_t min_key_;
    int64_t max_key_;

    SSTable(const filesystem::path &file_path);
    SSTable() = default;
    ~SSTable();

    Page *GetPage(off_t offset, bool is_sequential_flooding) const;

    optional<int64_t> Get(const int64_t key) const;
    vector<pair<int64_t, int64_t>> Scan(const int64_t start_key, const int64_t end_key) const;


private:
    off_t GetFileSize() const;
    void InitialKeyRange();

    bool ReadEntry(const char *buffer, size_t buffer_size, size_t &pos, pair<int64_t, int64_t> &entry) const;

    // Returns the offset of startKey or nullopt if not found
    optional<int64_t> BinarySearch(const int64_t key) const;

    // Returns the offset of startKey or its upper bound if not found
    int64_t BinarySearchUpperbound(const int64_t key, bool is_sequential_flooding) const;

    vector<pair<int64_t, int64_t>> LinearSearchToEndKey(off_t start_offset, int64_t start_key, int64_t end_key) const;
};


#endif // SSTABLE_H
