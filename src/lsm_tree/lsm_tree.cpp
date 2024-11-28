//
// Created by Kiiro Huang on 2024-11-24.
//

#include "lsm_tree.h"

#include <sys/fcntl.h>

#include "../../utils/log.h"
#include "../sst_counter.h"

using namespace std;
namespace fs = std::filesystem;

struct HeapNode {
    int64_t key;
    int64_t value;
    size_t page_index; // index of int64_t inside the current page
    size_t sst_id;

    bool operator>(const HeapNode &other) const { return key > other.key; }
};

vector<int64_t> LsmTree::SortMerge(vector<BTreeSSTable *> *ssts) {
    vector<int64_t> result;

    // Use priority_queue as a min-heap
    priority_queue<HeapNode, vector<HeapNode>, greater<>> min_heap;

    const size_t n = ssts->size();
    vector<vector<int64_t>> current_pages(n); // current page data
    vector<off_t> offsets(n, kPageNumReserveBTree * kPageSize); // current offset

    for (size_t i = 0; i < n; ++i) {
        auto &sst = (*ssts)[i];
        sst->fd_ = open(sst->file_path_.c_str(), O_RDONLY);
        const auto &page = sst->GetPage(offsets[i]);
        if (page->GetSize() > 0) {
            if (!page->data_.empty()) {
                current_pages[i] = page->data_;
            }

            size_t page_index = 0;
            const int64_t next_key = page->data_[page_index++];
            const int64_t next_value = page->data_[page_index++];
            min_heap.push({next_key, next_value, page_index, i});
        }
    }

    size_t last_sst_id = -1;
    while (!min_heap.empty()) {
        auto [key, value, page_index, sst_id] = min_heap.top();
        min_heap.pop();

        if (!result.empty() && result[result.size() - 2] == key) {
            // When key is duplicated, update value with the one from the latest SST
            if (sst_id > last_sst_id) {
                result.back() = value;
            }
        } else {
            // Add to the result
            result.push_back(key);
            result.push_back(value);
        }
        last_sst_id = sst_id;

        // Update min-heap
        auto &sst = (*ssts)[sst_id];
        auto &page = current_pages[sst_id];
        if (page_index + 2 <= page.size()) {
            // In current page, read next index
            const int64_t next_key = page[page_index++];
            const int64_t next_value = page[page_index++];
            min_heap.push({next_key, next_value, page_index, sst_id});
        } else if (page_index + kPairSize > page.size()) {
            // Read next page
            offsets[sst_id] += kPageSize;
            if (offsets[sst_id] >= sst->file_size_) {
                LOG("      Read EOF " << sst->file_path_);
                continue;
            }

            const auto &next_page = sst->GetPage(offsets[sst_id]);

            if (next_page) {
                min_heap.push({next_page->data_[0], next_page->data_[1], 2, sst_id});

                // Update newly read page into current_pages
                current_pages[sst_id] = next_page->data_;
            }
        }
    }

    return result;
}

// When full in previous level, Sort Merge all the ssts in this level
void LsmTree::SortMergePreviousLevel() {
    int current_level = 0;
    auto current_level_nodes = levelled_sst_[current_level];
    if (current_level_nodes.size() == kLsmRatio) {
        // needs to do the merge
        auto result = SortMerge(&current_level_nodes);
        for (auto &node: current_level_nodes) {
            DeleteFile(node->file_path_);
        }
        levelled_sst_[current_level].clear();

        // and then write to the next level
        if (levelled_sst_.size() == ++current_level) {
            levelled_sst_.push_back({});
        }
        auto next_level = levelled_sst_[current_level];

        if (next_level.size() < kLsmRatio - 1) {
            // If the next level is not full, add the result to the next level
            const string db_name = SSTCounter::GetInstance().GetDbName();
            BTreeSSTable new_sst_nodes = BTreeSSTable(db_name, true);
            levelled_sst_[current_level].push_back(&new_sst_nodes);

            // Generate a new SST in storage
            string file_path = new_sst_nodes.FlushToStorage(&result);

            // While the next level is about to be full, Sort Merge all the ssts in this level
            while (next_level.size() == kLsmRatio - 1) {
                auto next_result = SortMerge(&next_level);

                // Write to the "next" level
                next_level = levelled_sst_[current_level++];

                new_sst_nodes = BTreeSSTable(db_name, true);
                for (auto &node: next_level) {
                    DeleteFile(node->file_path_);
                }
                next_level.clear();
                next_level.push_back(&new_sst_nodes);

                // Generate a new SST in storage
                file_path = new_sst_nodes.FlushToStorage(&next_result);
            }
        }
    }
}

void LsmTree::DeleteFile(const string &file_path) {
    try {
        if (fs::exists(file_path)) {
            // Remove the file
            if (fs::remove(file_path)) {
                cout << "File " << file_path << " deleted successfully.\n";
            } else {
                cerr << "Failed to delete file: " << file_path << ".\n";
            }
        } else {
            cerr << "File does not exist: " << file_path << ".\n";
        }
    } catch (const fs::filesystem_error &e) {
        cerr << "Filesystem error: " << e.what() << ".\n";
    }
}


void LsmTree::SortMergeWritePreviousLevel(vector<BTreeSSTable *> *nodes, int current_level) {
    const auto result = SortMerge(nodes);

    auto next_level = levelled_sst_[current_level++];
    next_level = levelled_sst_[current_level++];

    const string db_name = SSTCounter::GetInstance().GetDbName();
    auto new_sst_nodes = BTreeSSTable(db_name, true);

    nodes->clear();
    next_level.push_back(&new_sst_nodes);

    // Generate a new SST in storage
    string file_path = new_sst_nodes.FlushToStorage(&result);
}

// Sort Merge every two SSTs in the final level
void LsmTree::SortMergeLastLevel() {
    auto last_level_nodes = levelled_sst_[level_];
    if (last_level_nodes.size() == 2) {
        SortMergeWriteLastLevel(&last_level_nodes);

        // const auto result = SortMerge(&last_level_nodes);
        //
        // const string db_name = SSTCounter::GetInstance().GetDbName();
        // auto new_sst_nodes = BTreeSSTable(db_name, true);
        //
        // // Remove the first two SSTs and add the new one
        // last_level_nodes.erase(last_level_nodes.begin());
        // last_level_nodes.erase(last_level_nodes.begin());
        // last_level_nodes.push_back(&new_sst_nodes);
        //
        // // Generate a new SST in storage
        // string file_path = new_sst_nodes.FlushToStorage(&result);
    }
}

void LsmTree::SortMergeWriteLastLevel(vector<BTreeSSTable *> *nodes) {
    const auto result = SortMerge(nodes);

    const string db_name = SSTCounter::GetInstance().GetDbName();
    auto new_sst_nodes = BTreeSSTable(db_name, true);

    nodes->clear();
    nodes->push_back(&new_sst_nodes);

    // Generate a new SST in storage
    string file_path = new_sst_nodes.FlushToStorage(&result);
}

vector<vector<BTreeSSTable>> LsmTree::ReadSSTsFromStorage() {}

// Build LSM-Tree from storage
void LsmTree::BuildLsmTree() {
    // Read all the ssts from the storage
    // levelled_sst_ = ReadSSTsFromStorage();

    // SortMergePreviousLevel();
    // SortMergeLastLevel();
    // FlushToNewLevel();
    // WriteSSTsToStorage();
}
