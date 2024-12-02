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

BTreeSSTable::BTreeSSTable(const string &db_name, const bool create_new, const int64_t level) :
    SSTable() {
    if (create_new) {
        // If creation, generate a new file name
        const string new_file_name = SSTCounter::GetInstance().GenerateFileName(level);
        file_path_ = fs::path(db_name) / new_file_name;
        fd_ = -1;
    } else {
        // If not creation, use the given file name
        file_path_ = fs::path(db_name);

        fd_ = open(file_path_.c_str(), O_RDONLY | O_CREAT, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open SSTable file: " + file_path_);
        }

        LOG("  Open file: " << file_path_);

        file_size_ = GetFileSize();
        BTreeSSTable::InitialKeyRange();
    }
}

void BTreeSSTable::InitialKeyRange() {
    char buffer[kPageSize];

    const size_t page_num_reserve_btree = ReadOffset();
    const off_t offset = page_num_reserve_btree * kPageSize;
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
    LOG("  └Writing page " << page->id_);

    // Write the page to the file
    const auto data = page->data_;

    int64_t buffer[min(kPagePairs * 2, data.size())];
    memset(buffer, 0, sizeof(buffer));

    // Change data to buffer
    for (size_t i = 0; i < data.size(); ++i) {
        buffer[i] = data[i];
    }

    const ssize_t bytes_written = pwrite(fd_, &buffer, sizeof(buffer), offset);
    if (bytes_written < 0) {
        cerr << "Failed to write page at offset " << offset << endl;
        exit(1);
    }

    // Write the page to the buffer pool
    const auto buffer_pool = BufferPoolManager::GetInstance();
    buffer_pool->Put(page->id_, data);
}


string BTreeSSTable::FlushToStorage(const vector<int64_t> *data) {
    CloseFile();

    fd_ = open(file_path_.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd_ < 0) {
        cerr << "Failed to open file: " << file_path_ << " for writing: " << strerror(errno) << endl;
        exit(1);
    }

    const size_t start_pos = file_path_.find('/') + 1;
    const size_t end_pos = file_path_.rfind(".bin");
    const string sst_name = file_path_.substr(start_pos, end_pos - start_pos);

    vector<int64_t> prev_layer_nodes;

    const size_t num_leaves = data->size() / (2 * kPagePairs);
    const size_t num_internal_nodes = max(static_cast<size_t>(1), num_leaves / kFanOut);

    // 1 page for root
    // 1 page for every 2nd layer node
    const size_t num_pages = 1 + num_internal_nodes;

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

        WritePage(offset, page);

        offset += kPageSize;
    }

    // Generate first 2 layers nodes
    GenerateBTreeLayers(prev_layer_nodes);

    // Root node writes to the first page
    WritePage(0, new Page(sst_name + "_0", root_));

    // Every second layer node writes to a new page
    for (size_t i = 0; i < internal_nodes_.size(); i++) {
        WritePage(kPageSize * (i + 1), new Page(sst_name + "_" + to_string(kPageSize * (i + 1)), internal_nodes_[i]));
    }

    LOG(" └Flushed to SST: " << file_path_);

    file_size_ = GetFileSize();
    min_key_ = (*data)[0];
    max_key_ = (*data)[data->size() - 2];

    return file_path_;
}

void BTreeSSTable::GenerateBTreeLayers(vector<int64_t> prev_layer_nodes) {
    internal_nodes_.clear();
    root_.clear();

    // Build internal nodes from leaf nodes (prev_layer_nodes)
    vector<vector<int64_t>> new_layer_nodes;
    size_t index = 0;
    size_t num_keys = prev_layer_nodes.size();

    // Create internal nodes with up to kFanOut keys
    while (index < num_keys) {
        size_t keys_in_node = std::min(kFanOut, num_keys - index);
        vector<int64_t> internal_node;

        for (size_t i = 0; i < keys_in_node; ++i) {
            internal_node.push_back(prev_layer_nodes[index + i]);
        }

        internal_nodes_.push_back(internal_node);
        new_layer_nodes.push_back(internal_node);
        index += keys_in_node;
    }

    // Now build the root node from internal nodes
    vector<int64_t> root_node_keys;
    for (const auto &node: new_layer_nodes) {
        // Use the last key of each internal node as the separator key
        root_node_keys.push_back(node.back());
    }

    // If root node has more keys than allowed, split it
    if (root_node_keys.size() > kFanOut) {
        // We need to create a new root node layer
        vector<vector<int64_t>> root_layer_nodes;
        index = 0;
        size_t num_root_keys = root_node_keys.size();

        while (index < num_root_keys) {
            size_t keys_in_node = std::min(kFanOut, num_root_keys - index);
            vector<int64_t> root_node;

            for (size_t i = 0; i < keys_in_node; ++i) {
                root_node.push_back(root_node_keys[index + i]);
            }

            root_layer_nodes.push_back(root_node);
            index += keys_in_node;
        }

        // Since we only have three layers, the root must be a single node
        // So we need to merge the root_layer_nodes into a single root node
        // and move the previous root_layer_nodes to internal_nodes_
        if (root_layer_nodes.size() == 1) {
            // The root node fits in one node
            root_ = root_layer_nodes[0];
        } else {
            // We need to create a new root node with the last keys of root_layer_nodes
            vector<int64_t> new_root_node;
            for (const auto &node: root_layer_nodes) {
                new_root_node.push_back(node.back());
            }

            // The root node must fit in one node
            if (new_root_node.size() > kFanOut) {
                cerr << "Error: Root node exceeds maximum capacity of " << kFanOut << " keys." << endl;
                exit(1);
            }

            root_ = new_root_node;

            // Move the previous root_layer_nodes to internal_nodes_
            internal_nodes_.clear();
            for (const auto &node: root_layer_nodes) {
                internal_nodes_.push_back(node);
            }
        }
    } else {
        // Root node fits in one node
        root_ = root_node_keys;
    }
}

off_t BTreeSSTable::ReadOffset() const {
    // Read first page of the BTreeSSTable, get the number of second layer nodes
    const Page *page = GetPage(0);
    if (!page) {
        cerr << "Failed to read page 0 for ReadOffset: " << file_path_ << endl;
        exit(1);
    }

    const auto data = page->data_;
    for (auto i = 0; i < data.size(); i++) {
        if (data[i] == 0) {
            return 1 + i;
        }
    }

    return 1 + data.size();
}

optional<int64_t> BTreeSSTable::BinarySearch(const int64_t key) const {
    const size_t num_pages = (file_size_ + kPageSize - 1) / kPageSize;

    fd_ = open(file_path_.c_str(), O_RDONLY);
    const size_t page_num_reserve_btree = ReadOffset();
    size_t left = page_num_reserve_btree;
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
            // Since the key is already in order, do another binary search to search inside the page
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

    LOG("  Could not find key " << key << " in " << file_path_);
    return nullopt;
}

int64_t BTreeSSTable::BinarySearchUpperbound(const int64_t key, bool is_sequential_flooding) const {
    const size_t num_pages = (file_size_ + kPageSize - 1) / kPageSize;

    const size_t page_num_reserve_btree = ReadOffset();
    size_t left = page_num_reserve_btree;
    size_t right = num_pages;

    // Outer binary search to find the first page where first_key > key
    while (left < right) {
        const size_t mid = left + (right - left) / 2;
        const off_t offset = mid * kPageSize;

        const Page *page = GetPage(offset, is_sequential_flooding);
        const auto data = page->data_;

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
