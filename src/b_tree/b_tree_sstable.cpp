//
// Created by Kiiro Huang on 24-11-21.
//

#include "b_tree_sstable.h"

#include <regex>
#include <sys/_types/_off_t.h>
#include <sys/fcntl.h>
#include <unistd.h>

#include "../../utils/constants.h"
#include "../../utils/log.h"
#include "../buffer_pool/buffer_pool_manager.h"
#include "../buffer_pool/page.h"
#include "../memtable.h"
#include "../sst_counter.h"

namespace fs = std::filesystem;

BTreeSSTable::BTreeSSTable(const string &db_name, const bool create_new, const int64_t level) : SSTable() {
    if (create_new) {
        // If creation, generate a new file name
        const string new_file_name = SSTCounter::GetInstance().GenerateFileName(level);
        file_path_ = fs::path(db_name) / new_file_name;
    } else {
        // If not creation, use the given file name
        file_path_ = fs::path(db_name);
    }

    fd_ = open(file_path_.c_str(), O_RDONLY | O_CREAT, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open SSTable file.");
    }

    LOG("  Open file: " << file_path_);

    if (!create_new) {
        file_size_ = GetFileSize();
        InitialKeyRange();
    }
}

void BTreeSSTable::InitialKeyRange() {
    char buffer[kPageSize];

    const off_t offset = kPageNumReserveBTree * kPageSize;
    // Read the first block to get the minimum key
    ssize_t bytes_read = pread(fd_, buffer, kPageSize, offset);
    if (bytes_read > 0) {
        size_t pos = 0;
        pair<int64_t, int64_t> first_entry;
        if (ReadEntry(buffer, bytes_read, pos, first_entry)) {
            min_key_ = first_entry.first;
        }
    }

    // Read the last block to get the maximum key
    const off_t last_block_offset = file_size_ > offset ? file_size_ - kPageSize : offset;
    bytes_read = pread(fd_, buffer, kPageSize, last_block_offset);
    if (bytes_read > 0) {
        size_t pos = 0;
        pair<int64_t, int64_t> last_entry;
        while (pos < static_cast<size_t>(bytes_read) && ReadEntry(buffer, bytes_read, pos, last_entry)) {
            // Keep reading to get the last entry in the block
        }
        max_key_ = last_entry.first;
    }
}


void BTreeSSTable::WritePage(const off_t offset, const Page *page, const bool is_final_page = false) const {
    // Write the page to the file
    const auto data = page->data_;

    // alignas(8) char buffer[kPageSize];

    // int64_t buffer[kPagePairs * 2];
    // if (!is_final_page) {
    //     alignas(8) int64_t buffer[kPagePairs * 2];
    // } else {
    //     int64_t buffer[min(kPagePairs * 2, data.size())];
    // }
    // memset(buffer, 0, sizeof(buffer));

    // int64_t buffer[kPagePairs * 2];
    int64_t buffer[min(kPagePairs * 2, data.size())];
    memset(buffer, 0, sizeof(buffer));

    // data to buffer
    for (size_t i = 0; i < data.size(); ++i) {
        buffer[i] = data[i];
    }

    // ssize_t bytes_written;
    // if (!is_final_page) {
    //     bytes_written = pwrite(fd_, &buffer, kPageSize, offset);
    // } else {
    //     bytes_written = pwrite(fd_, &buffer, sizeof(buffer), offset);
    // }

    const ssize_t bytes_written = pwrite(fd_, &buffer, sizeof(buffer), offset);
    // const ssize_t bytes_written = pwrite(fd_, &buffer, kPageSize, offset);

    if (bytes_written < 0) {
        cerr << "Failed to write page at offset " << offset << endl;
        exit(1);
    }

    // Write the page to the buffer pool
    const auto buffer_pool = BufferPoolManager::GetInstance();
    buffer_pool->Put(page->id_, data);
}


string BTreeSSTable::FlushToStorage(const vector<int64_t> *data) {
    fd_ = open(file_path_.c_str(), O_WRONLY | O_CREAT, 0644);

    const size_t start_pos = file_path_.find('/') + 1;
    const size_t end_pos = file_path_.rfind(".bin");
    const string sst_name = file_path_.substr(start_pos, end_pos - start_pos);

    vector<int64_t> prev_layer_nodes;

    // 1 page for root
    // 1 page for every 2nd layer node (now it's 2)
    const size_t num_pages = 1 + 2;

    // Offset are left for the first 2 layers of the B-Tree
    const off_t start_offset = num_pages * kPageSize;
    off_t offset = start_offset;

    // Write the data to B-Tree leaf nodes
    while (offset < start_offset + data->size() * kPairSize / 2) {
        vector<int64_t> page_data;
        page_data.clear();

        string page_id = sst_name + "_" + to_string(offset);
        for (size_t j = (offset - start_offset) * 2 / kPairSize;
             j < (offset - start_offset) * 2 / kPairSize + kPagePairs * 2 && j < data->size(); j++) {
            page_data.push_back((*data)[j]);
        }
        LOG("  ┌-Current page size: " << page_data.size());
        const auto page = new Page(page_id, page_data);

        // Fetch the last key of every page
        if (!page_data.empty()) {
            const int64_t last_key = page_data[page_data.size() - 2];
            LOG("  | Last key: " << last_key);
            prev_layer_nodes.push_back(last_key);
        }

        LOG("  └Writing page " << page_id);
        WritePage(offset, page);

        offset += kPageSize;
    }

    // Generate first 2 layers nodes
    GenerateBTreeLayers(prev_layer_nodes);

    // Root node writes to the first page
    WritePage(0, new Page(sst_name + "_0", root_));

    // Every second layer node writes to a new page
    for (size_t i = 0; i < internal_nodes_.size(); i++) {
        WritePage(kPageSize * (i + 1), new Page(sst_name + "_" + to_string(i + 1), internal_nodes_[i]));
    }

    LOG(" └Flushed to SST: " << file_path_);

    file_size_ = GetFileSize();
    min_key_ = (*data)[0];
    max_key_ = (*data)[data->size() - 2];

    return file_path_;
}

void BTreeSSTable::GenerateBTreeLayers(vector<int64_t> prev_layer_nodes) {
    const size_t num_nodes = prev_layer_nodes.size();

    const int64_t root_index = num_nodes / 2 - 1;
    const int64_t root = prev_layer_nodes[root_index];
    root_.push_back(root);

    vector<int64_t> temp;
    for (size_t i = 0; i < num_nodes - 1; i++) {
        if (i < root_index) {
            temp.push_back(prev_layer_nodes[i]);
        } else if (i == root_index) {
            internal_nodes_.push_back(temp);
            temp.clear();
        } else {
            temp.push_back(prev_layer_nodes[i]);
        }
    }
    internal_nodes_.push_back(temp);
}

optional<int64_t> BTreeSSTable::BinarySearch(const int64_t key) const {
    const size_t num_pages = (file_size_ + kPageSize - 1) / kPageSize;

    size_t left = kPageNumReserveBTree;
    size_t right = num_pages - 1;

    while (left <= right) {
        const size_t mid = left + (right - left) / 2;
        const off_t offset = mid * kPageSize;

        const Page *page = GetPage(offset);
        const auto data = page->data_;
        const size_t num_pairs = page->GetSize() / 2;

        const int64_t first_key = data[0];
        const int64_t last_key = data[(num_pairs - 1) * 2];

        if (key == first_key) {
            LOG("\t\tFound key " << key << " in " << file_path_);
            return data[1];
        }
        if (key == last_key) {
            LOG("\t\tFound key " << key << " in " << file_path_);
            return data[num_pairs * 2 - 1];
        }

        if (key > first_key && key < last_key) {
            // since the key is already in order, do another binary search to search inside the page
            size_t page_left = 0;
            size_t page_right = num_pairs - 1;

            while (page_left <= page_right) {
                const size_t page_mid = page_left + (page_right - page_left) / 2;
                const int64_t mid_key = data[page_mid * 2];

                if (mid_key == key) {
                    LOG("\t\tFound key " << key << " in " << file_path_);
                    return data[page_mid * 2 + 1];
                }

                if (mid_key < key) {
                    page_left = page_mid + 1;
                } else {
                    page_right = page_mid - 1;
                }
            }

            return nullopt;
        }

        if (key < first_key) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    LOG("  2 Could not find key " << key << " in " << file_path_);
    // ::close(fd_);
    return nullopt;
}

int64_t BTreeSSTable::BinarySearchUpperbound(const int64_t key, bool is_sequential_flooding) const {
    const size_t num_pages = (file_size_ + kPageSize - 1) / kPageSize;

    size_t left = kPageNumReserveBTree;
    size_t right = num_pages;

    // Outer binary search to find the first page where first_key > key
    while (left < right) {
        const size_t mid = left + (right - left) / 2;
        const off_t offset = mid * kPageSize;

        const Page *page = GetPage(offset, is_sequential_flooding);
        const auto data = page->data_;
        const size_t num_pairs = page->GetSize() / 2;

        const int64_t first_key = data[0];

        if (first_key <= key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if (left - 1 == num_pages) {
        // The key is greater than or equal to all keys in the SSTable
        return -1;
    }

    // Read the page where the upper bound could be
    const size_t page_index = left - 1;
    const off_t page_offset = page_index * kPageSize;

    const Page *page = GetPage(page_offset, is_sequential_flooding);
    const auto data = page->data_;
    const size_t num_pairs = page->GetSize() / 2;

    const int64_t first_key = data[0];
    const int64_t last_key = data[(num_pairs - 1) * 2];

    // Inner binary search to find the upper bound within the page
    size_t page_left = 0;
    size_t page_right = num_pairs;

    while (page_left < page_right) {
        const size_t page_mid = page_left + (page_right - page_left) / 2;
        const int64_t mid_key = data[page_mid * 2];

        if (mid_key <= key) {
            page_left = page_mid + 1;
        } else {
            page_right = page_mid;
        }
    }

    if (page_left - 1 == num_pairs) {
        // All keys in this page are less than or equal to the given key
        // Check if there is a next page
        if (page_index + 1 <= num_pages) {
            if (page_index + 1 < num_pages) {
                const off_t next_page_offset = (page_index + 1) * kPageSize;
                return next_page_offset; // Upper bound starts at the next page
            }
            // No more keys available
            return -1;
        }
        // No next page; the given key is greater than all keys
        return -1;
    }

    // Upper bound found within the page
    const off_t key_offset = page_offset + (page_left - 1) * kPairSize;
    return key_offset;
}

vector<pair<int64_t, int64_t>> BTreeSSTable::LinearSearchToEndKey(off_t start_offset, int64_t start_key,
                                                                  int64_t end_key, bool is_sequential_flooding) const {
    vector<pair<int64_t, int64_t>> result;

    auto current_offset = start_offset;

    while (true) {
        const Page *page = GetPage(current_offset, is_sequential_flooding);

        // When start key is the last key in the SSTable, next page will be nullptr
        if (page == nullptr) {
            return result;
        }

        const auto data = page->data_;
        const size_t num_pairs = page->GetSize() / 2;

        for (size_t i = 0; i < num_pairs; i++) {
            if (data[i * 2] > end_key) {
                return result;
            }

            if (data[i * 2] >= start_key) {
                result.emplace_back(data[i * 2], data[i * 2 + 1]);
            }
        }

        current_offset += kPageSize;
    }
}
