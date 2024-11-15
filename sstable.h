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

    void initial_key_range();

    off_t get_file_size() const;
    bool read_entry(const char *buffer, size_t buffer_size, size_t &pos, Entry &entry) const;
    bool read_page(uint64_t offset, std::vector<Entry> &entries, int64_t first_key, int64_t last_key) const;

    // Returns the offset of startKey or nullopt if not found
    optional<int64_t> binary_search(const int64_t key) const;

    vector<pair<int64_t, int64_t>> scan(const int64_t start_key, const int64_t end_key) const;

    // Returns the offset of startKey or its upper bound if not found
    int64_t binary_search_upperbound(const int64_t key) const;

    vector<pair<int64_t, int64_t>> linear_search_to_end_key(off_t start_offset, int64_t end_key) const;
};


#endif // SSTABLE_H
