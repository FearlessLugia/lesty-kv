//
// Created by Kiiro Huang on 2024-11-24.
//

#include "lsm_tree.h"

#include <sys/fcntl.h>

#include "../../utils/log.h"
#include "../sst_counter.h"
#include "lsm_tree.h"

#include <cassert>

using namespace std;
namespace fs = std::filesystem;


LsmTree::LsmTree() { levelled_sst_.clear(); }

LsmTree &LsmTree::GetInstance() {
    static LsmTree instance;
    return instance;
}

vector<int64_t> LsmTree::SortMerge(vector<BTreeSSTable> *ssts) {
    LOG(" ┌Sort Merge " << (*ssts)[0].file_path_ << " to " << (*ssts)[ssts->size() - 1].file_path_);

    vector<int64_t> result;

    // Use priority_queue as a min-heap
    priority_queue<HeapNode, vector<HeapNode>, greater<>> min_heap;

    const size_t n = ssts->size();
    vector<vector<int64_t>> current_pages(n); // current page data
    vector<off_t> offsets(n, kPageNumReserveBTree * kPageSize); // current offset

    for (size_t i = 0; i < n; ++i) {
        auto &sst = (*ssts)[i];
        sst.fd_ = open(sst.file_path_.c_str(), O_RDONLY);
        const auto &page = sst.GetPage(offsets[i]);
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
        } else {
            // Read next page
            offsets[sst_id] += kPageSize;
            if (offsets[sst_id] >= sst.file_size_) {
                LOG("    Read EOF " << sst.file_path_);
                continue;
            }

            const auto &next_page = sst.GetPage(offsets[sst_id]);

            if (next_page) {
                min_heap.push({next_page->data_[0], next_page->data_[1], 2, sst_id});

                // Update newly read page into current_pages
                current_pages[sst_id] = next_page->data_;
            }
        }
    }

    for (size_t i = 2; i < result.size(); i += 2) {
        if (result[i] <= result[i - 2]) {
            LOG(result[i] << " <= " << result[i - 2]);
            assert(false);
        }
    }

    return result;
}

void LsmTree::AddSst(BTreeSSTable sst) {
    // Ensure the first level is not empty
    if (levelled_sst_.empty()) {
        levelled_sst_.resize(1);
    }

    levelled_sst_[0].push_back(sst);
}

// When full in previous level, Sort Merge all the SSTs in this level
void LsmTree::SortMergePreviousLevel(int64_t current_level) {
    // If the current level is full
    if (levelled_sst_[current_level].size() == pow(kLsmRatio, current_level + 1)) {
        // needs to do the merge
        const auto result = SortMerge(&levelled_sst_[current_level]);

        for (auto &node: levelled_sst_[current_level]) {
            DeleteFile(node.file_path_);
        }
        levelled_sst_[current_level].clear();

        // and then write to the next level
        if (levelled_sst_.size() == current_level + 1) {
            levelled_sst_.push_back({});
        }

        const int64_t next_level = current_level + 1;
        // if (levelled_sst_[next_level].size() < kLsmRatio - 1) {

        // Add the result to the next level
        string db_name = SSTCounter::GetInstance().GetDbName();
        BTreeSSTable new_sst_nodes = BTreeSSTable(db_name, true, next_level);

        // Generate a new SST in storage
        string file_path = new_sst_nodes.FlushToStorage(&result);

        levelled_sst_[next_level].push_back(new_sst_nodes);
        // }

        // // While the next level is about to be full, Sort Merge all the ssts in this level
        // while (levelled_sst_[next_level].size() == kLsmRatio - 1) {
        //     auto next_result = SortMerge(&levelled_sst_[next_level]);
        //
        //     // Write to the "next" level
        //     // next_level = levelled_sst_[current_level++];
        //
        //     string db_name = SSTCounter::GetInstance().GetDbName();
        //     BTreeSSTable new_sst_nodes = BTreeSSTable(db_name, true, next_level);
        //     for (auto &node: levelled_sst_[next_level]) {
        //         DeleteFile(node.file_path_);
        //     }
        //     levelled_sst_[next_level].clear();
        //     levelled_sst_[next_level].push_back(new_sst_nodes);
        //
        //     // Generate a new SST in storage
        //     auto file_path = new_sst_nodes.FlushToStorage(&next_result);
        // }
    }
}


// void LsmTree::SortMergeWritePreviousLevel(vector<BTreeSSTable> *nodes, int current_level) {
//     const auto result = SortMerge(nodes);
//
//     if (levelled_sst_.size() <= current_level + 1) {
//         levelled_sst_.resize(current_level + 2);
//     }
//
//     auto &next_level = levelled_sst_[current_level + 1];
//
//     const string db_name = SSTCounter::GetInstance().GetDbName();
//     auto new_sst_nodes = BTreeSSTable(db_name, true);
//
//     nodes->clear();
//     next_level.push_back(new_sst_nodes);
//
//     // Generate a new SST in storage
//     string file_path = new_sst_nodes.FlushToStorage(&result);
// }

// Sort Merge every two SSTs in the final level
void LsmTree::SortMergeLastLevel() {
    auto last_level_nodes = levelled_sst_[kLevelToApplyDostoevsky];
    if (last_level_nodes.size() >= 2) {
        const auto result = SortMerge(&last_level_nodes);

        for (auto &node: levelled_sst_[kLevelToApplyDostoevsky]) {
            DeleteFile(node.file_path_);
        }
        levelled_sst_[kLevelToApplyDostoevsky].clear();

        // Name of SST should always be BTree_lastlevel_0.bin
        const string db_name = SSTCounter::GetInstance().GetDbName();
        auto new_sst_nodes = BTreeSSTable(db_name, true, kLevelToApplyDostoevsky);
        levelled_sst_[kLevelToApplyDostoevsky].push_back(new_sst_nodes);

        // Generate a new SST in storage
        string file_path = new_sst_nodes.FlushToStorage(&result);
    }
}

// void LsmTree::SortMergeWriteLastLevel(vector<BTreeSSTable> *nodes) {
//     const auto result = SortMerge(nodes);
//
//     const string db_name = SSTCounter::GetInstance().GetDbName();
//     auto new_sst_nodes = BTreeSSTable(db_name, true);
//
//     nodes->clear();
//     nodes->push_back(new_sst_nodes);
//
//     // Generate a new SST in storage
//     string file_path = new_sst_nodes.FlushToStorage(&result);
// }

vector<vector<BTreeSSTable>> LsmTree::ReadSSTsFromStorage() {
    vector<vector<BTreeSSTable>> levels;
    vector<int64_t> level_counters;

    auto &sst_counter = SSTCounter::GetInstance();
    const string db_name = sst_counter.GetDbName();

    const regex filename_pattern(R"(btree(\d+)_(\d+)\.bin)");

    for (const auto &entry: fs::directory_iterator(db_name)) {
        string filename = entry.path().filename().string();
        smatch match;
        if (regex_match(filename, match, filename_pattern)) {
            const int64_t level = stoll(match[1].str());
            const int64_t index = stoll(match[2].str());

            if (level >= levels.size()) {
                levels.resize(level + 1);
            }
            if (level >= level_counters.size()) {
                level_counters.resize(level + 1, 0);
            }

            BTreeSSTable sst(db_name + "/" + filename, false);
            levels[level].push_back(sst);

            level_counters[level] = max(level_counters[level], index + 1);
            sst_counter.SetLevelCounters(level, level_counters[level]);
        }
    }

    return levels;
}

// Build LSM-Tree from storage
void LsmTree::BuildLsmTree() {
    // Read all the SSTs from the storage
    levelled_sst_ = ReadSSTsFromStorage();

    // Do necessary sort merge
    OrderLsmTree();
}

void LsmTree::OrderLsmTree() {
    for (int64_t current_level = 0; current_level < min(levelled_sst_.size(), kLevelToApplyDostoevsky);
         current_level++) {
        SortMergePreviousLevel(current_level);
    }

    if (kLevelToApplyDostoevsky == levelled_sst_.size() - 1) {
        SortMergeLastLevel();
    }
}

void LsmTree::DeleteFile(const string &file_path) {
    try {
        if (fs::exists(file_path)) {
            // Remove the file
            if (fs::remove(file_path)) {
                LOG("  File " << file_path << " deleted successfully");
            } else {
                cerr << "Failed to delete file: " << file_path << endl;
            }
        } else {
            cerr << "File does not exist: " << file_path << endl;
        }
    } catch (const fs::filesystem_error &e) {
        cerr << "Filesystem error: " << e.what() << endl;
    }
}
