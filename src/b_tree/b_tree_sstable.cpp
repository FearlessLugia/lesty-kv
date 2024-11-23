//
// Created by Kiiro Huang on 24-11-21.
//

#include "b_tree_sstable.h"

#include <sys/_types/_off_t.h>
#include <sys/fcntl.h>
#include <unistd.h>

#include "../../utils/constants.h"
#include "../../utils/log.h"
#include "../buffer_pool/buffer_pool_manager.h"
#include "../buffer_pool/page.h"
#include "../memtable.h"

BTreeSSTable::BTreeSSTable(const filesystem::path &file_path) : SSTable() { file_path_ = file_path; }


bool BTreeSSTable::WriteEntry(char *buffer, const size_t buffer_size, size_t &pos, pair<int64_t, int64_t> &entry) {
    // Ensure there is enough space in the buffer for both key and value (2 x int64_t)
    if (pos + 2 * sizeof(int64_t) > buffer_size) {
        return false; // Not enough data left in the buffer
    }

    // Read key as int64_t
    memcpy(buffer + pos, &entry.first, sizeof(int64_t));
    pos += sizeof(int64_t);

    // Read value as int64_t
    memcpy(buffer + pos, &entry.second, sizeof(int64_t));
    pos += sizeof(int64_t);

    return true;
}

void BTreeSSTable::WritePage(const off_t offset, const Page *page) const {
    // Write the page to the file
    const auto data = page->data_;

    // alignas(8) char buffer[kPageSize];
    int64_t buffer[min(kPagePairs * 2,data.size())];
    memset(buffer, 0, sizeof(buffer));

    // data to buffer
    for (size_t i = 0; i < data.size(); ++i) {
        buffer[i] = data[i];
    }

    const ssize_t bytes_written = pwrite(fd_, &buffer, sizeof(buffer), offset);

    if (bytes_written < 0) {
        cerr << "Failed to write page at offset " << offset << endl;
        exit(1);
    }
    // LOG("    Page written at offset " << offset << " with " << page->data_.size() / 2 << " key-value pairs.");

    // Write the page to the buffer pool
    auto buffer_pool = BufferPoolManager::GetInstance();
}


void BTreeSSTable::FlushFromMemtable(const vector<int64_t> *data) {
    // 1 memtable -> 1 SSTable
    fd_ = open(file_path_.c_str(), O_WRONLY | O_CREAT, 0644);

    const size_t start_pos = file_path_.find('/') + 1;
    const size_t end_pos = file_path_.rfind(".bin");
    const string sst_name = file_path_.substr(start_pos, end_pos - start_pos);

    off_t offset = 0;
    while (offset < data->size() * kPairSize / 2) {
        vector<int64_t> page_data;
        page_data.clear();

        string page_id = sst_name + "_" + to_string(offset);
        for (size_t j = offset * 2 / kPairSize; j < offset * 2 / kPairSize + kPagePairs * 2 && j < data->size(); j++) {
            if ((*data)[j] == 5000) {
                int a = 1;
            }
            page_data.push_back((*data)[j]);
        }
        LOG("    Current page size: " << page_data.size());
        const auto page = new Page(page_id, page_data);

        LOG("  Writing page " << page_id << " at offset " << offset);
        WritePage(offset, page);

        offset += kPageSize;
    }


    LOG("  Memtable flushed to SST: " << file_path_);

    // outfile.close();
}
