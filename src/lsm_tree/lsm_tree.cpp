//
// Created by Kiiro Huang on 2024-11-24.
//

#include "lsm_tree.h"
#include "../buffer_pool/buffer_pool_manager.h"

#include <sys/fcntl.h>

#include "../../utils/log.h"
#include "../sst_counter.h"
#include "lsm_tree.h"
#include "../buffer_pool/buffer_pool_manager.h"

#include <cassert>

using namespace std;
namespace fs = std::filesystem;


LsmTree::LsmTree() { BuildLsmTree(); }

LsmTree::~LsmTree() {
    for (auto &level: levelled_sst_) {
        for (auto &sst: level) {
            delete sst;
        }
        level.clear();
    }
    levelled_sst_.clear();
}

LsmTree &LsmTree::GetInstance() {
    static LsmTree instance;
    return instance;
}

vector<int64_t> LsmTree::SortMerge(vector<BTreeSSTable *> *ssts, bool should_dispose_tombstone) {
    LOG(" ┌Sort Merge " << (*ssts)[0]->file_path_ << " to " << (*ssts)[ssts->size() - 1]->file_path_);

    vector<int64_t> result;

    // Use priority_queue as a min-heap
    priority_queue<HeapNode, vector<HeapNode>, greater<>> min_heap;

    const size_t n = ssts->size();
    vector<vector<int64_t>> current_pages(n); // current page data

    vector<off_t> offsets(n, 0); // current offset
    // Read first page of each SSTable to get offset
    for (size_t i = 0; i < n; ++i) {
        auto &sst = (*ssts)[i];

        // const int fd = sst->file_handler_.GetFd();
        // if (fd < 0) {
            // throw runtime_error("Failed to open file: " + sst->file_path_);
        // }

        // const int fd = sst->file_handler_.GetFd();
        // sst->fd_ = sst->OpenFile();
        sst->fd_ = open(sst->file_path_.c_str(), O_RDONLY);
        auto offset_page = sst->ReadOffset();
        offsets[i] = offset_page * kPageSize;
    }

    for (size_t i = 0; i < n; ++i) {
        auto &sst = (*ssts)[i];
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

    while (!min_heap.empty()) {
        auto [key, value, page_index, sst_id] = min_heap.top();
        min_heap.pop();

        // If largest level, should dispose tombstone
        if (should_dispose_tombstone && value == INT64_MIN) {
            continue;
        }

        // Add to the result
        // When key is duplicated, its sst_id would surely be smaller than the previous one
        if (result.empty() || result[result.size() - 2] != key) {
            result.push_back(key);
            result.push_back(value);
        }

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
            if (offsets[sst_id] >= sst->file_size_) {
                LOG("    Read EOF " << sst->file_path_);
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

    for (size_t i = 2; i < result.size(); i += 2) {
        if (result[i] <= result[i - 2]) {
            LOG(result[i] << " <= " << result[i - 2]);
            // assert(false);
        }
    }

    return result;
}

void LsmTree::AddSst(BTreeSSTable *sst) {
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
        LOG(" Sort Merge Previous Level " << current_level);

        // needs to do the merge
        const auto result = SortMerge(&levelled_sst_[current_level], false);

        for (const auto &node: levelled_sst_[current_level]) {
            DeleteFile(node);

            // Remove the pages from buffer pool
            BufferPool *buffer_pool = BufferPoolManager::GetInstance();
            buffer_pool->RemoveLevel(current_level);
        }
        levelled_sst_[current_level].clear();

        // Current level cleared, set current level counter to 0
        SSTCounter::GetInstance().SetLevelCounters(current_level, 0);

        // and then write to the next level
        if (levelled_sst_.size() == current_level + 1) {
            levelled_sst_.push_back({});
        }

        const int64_t next_level = current_level + 1;

        // Add the result to the next level
        const string db_name = SSTCounter::GetInstance().GetDbName();
        const auto new_sst_nodes = new BTreeSSTable(db_name, true, next_level);

        // Generate a new SST in storage
        string file_path = new_sst_nodes->FlushToStorage(&result);

        levelled_sst_[next_level].push_back(new_sst_nodes);
    }
}

// Sort Merge every two SSTs in the final level
void LsmTree::SortMergeLastLevel() {
    auto last_level_nodes = levelled_sst_[kLevelToApplyDostoevsky];
    if (last_level_nodes.size() >= 2) {
        const auto result = SortMerge(&last_level_nodes, true);

        for (const auto &node: levelled_sst_[kLevelToApplyDostoevsky]) {
            DeleteFile(node);

            // Remove the pages from buffer pool
            BufferPool *buffer_pool = BufferPoolManager::GetInstance();
            buffer_pool->RemoveLevel(kLevelToApplyDostoevsky);
        }
        levelled_sst_[kLevelToApplyDostoevsky].clear();

        // Name of SST should always be BTree_lastlevel_0.bin
        const string db_name = SSTCounter::GetInstance().GetDbName();
        const auto new_sst_nodes = new BTreeSSTable(db_name, true, kLevelToApplyDostoevsky);
        levelled_sst_[kLevelToApplyDostoevsky].push_back(new_sst_nodes);

        // Generate a new SST in storage
        string file_path = new_sst_nodes->FlushToStorage(&result);
    }
}

vector<vector<BTreeSSTable *>> LsmTree::ReadSSTsFromStorage() {
    vector<vector<BTreeSSTable *>> levels;
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

            auto sst = new BTreeSSTable(db_name + "/" + filename, false);
            levels[level].push_back(sst);

            level_counters[level] = max(level_counters[level], index + 1);
            sst_counter.SetLevelCounters(level, level_counters[level]);
        }
    }

    // Sort the SSTs in each level, the oldest SST comes first
    for (auto &level: levels) {
        ranges::sort(level, [](const BTreeSSTable *a, const BTreeSSTable *b) { return a->file_path_ < b->file_path_; });
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

void LsmTree::DeleteFile(BTreeSSTable *sst) {
    try {
        // sst->CloseFile();

        const string &file_path = sst->file_path_;
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

        delete sst;
    } catch (const fs::filesystem_error &e) {
        cerr << "Filesystem error: " << e.what() << endl;
    }
}
