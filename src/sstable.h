//
// Created by Kiiro on 24-11-4.
//

#ifndef SSTABLE_H
#define SSTABLE_H
#include <fstream>

using namespace std;
class SSTable {
public:
    string file_path_;
    int fd_;
    off_t file_size_;

    struct Entry {
        int64_t key_;
        int64_t value_;
    };
    vector<Entry> entries_;

    int64_t min_key_;
    int64_t max_key_;


    SSTable(const filesystem::path &file_path);
    ~SSTable();

    bool ReadEntry(const char *buffer, size_t buffer_size, size_t &pos, Entry &entry) const;
    bool ReadPage(uint64_t offset, std::vector<Entry> &entries, int64_t first_key, int64_t last_key) const;

    optional<int64_t> Get(const int64_t key) const;
    vector<pair<int64_t, int64_t>> Scan(const int64_t start_key, const int64_t end_key) const;


private:
    off_t GetFileSize() const;
    void InitialKeyRange();

    // Returns the offset of startKey or nullopt if not found
    optional<int64_t> BinarySearch(const int64_t key) const;

    // Returns the offset of startKey or its upper bound if not found
    int64_t BinarySearchUpperbound(const int64_t key) const;

    vector<pair<int64_t, int64_t>> LinearSearchToEndKey(off_t start_offset, int64_t end_key) const;
};


#endif // SSTABLE_H
