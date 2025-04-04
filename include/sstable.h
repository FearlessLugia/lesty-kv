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
    off_t file_size_;

    mutable int fd_;

    int64_t min_key_;
    int64_t max_key_;

    SSTable() = default;
    ~SSTable();

    int EnsureFileOpen() const;
    void CloseFile() const;

    Page *GetPage(off_t offset, bool is_sequential_flooding = false) const;

    optional<int64_t> Get(int64_t key) const;
    vector<pair<int64_t, int64_t>> Scan(int64_t start_key, int64_t end_key) const;


protected:
    off_t GetFileSize() const;
    virtual void InitialKeyRange();

    bool ReadEntry(const char *buffer, size_t buffer_size, size_t &pos, pair<int64_t, int64_t> &entry) const;

    // Returns the offset of startKey or nullopt if not found
    virtual optional<int64_t> BinarySearch(const int64_t key) const;

    // Returns the offset of startKey or its upper bound if not found
    virtual int64_t BinarySearchUpperbound(const int64_t key, bool is_sequential_flooding) const;

    virtual vector<pair<int64_t, int64_t>> LinearSearchToEndKey(off_t start_offset, int64_t start_key, int64_t end_key,
                                                                bool is_sequential_flooding) const;
};


#endif // SSTABLE_H
