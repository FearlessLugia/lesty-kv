//
// Created by Kiiro Huang on 2024-11-24.
//

#include "lsm_tree.h"

#include <sys/fcntl.h>

#include "../../utils/log.h"

using namespace std;

struct HeapNode {
    int64_t key;
    int64_t value;
    size_t page_index; // index of int64_t inside the current page
    size_t sst_id;

    bool operator>(const HeapNode &other) const { return key > other.key; }
};

vector<pair<int64_t, int64_t>> LsmTree::SortMerge(vector<BTreeSSTable> *ssts) {
    // Use priority_queue as a min-heap
    priority_queue<HeapNode, vector<HeapNode>, greater<>> min_heap;

    const size_t n = ssts->size();
    vector<std::vector<int64_t>> current_pages(n); // current page data
    vector<off_t> offsets(n, kPageNumReserveBTree * kPageSize); // current offset

    vector<pair<int64_t, int64_t>> result;
    BTreeSSTable merged_table = BTreeSSTable("db1/btree_merged.bin");

    for (size_t i = 0; i < n; ++i) {
        auto &sst = (*ssts)[i];
        sst.fd_ = open(sst.file_path_.c_str(), O_RDONLY);
        const auto &page = sst.GetPage(offsets[i]);
        if (page->GetSize() > 0) {
            auto page = sst.GetPage(offsets[i]);
            if (!page->data_.empty()) {
                current_pages[i] = page->data_;
            }

            min_heap.push({current_pages[i][0], current_pages[i][1], 2, i});
        }
    }

    size_t last_sst_id = -1;
    while (!min_heap.empty()) {
        auto [key, value, page_index, sst_id] = min_heap.top();
        min_heap.pop();

        // Add to the result
        if (!result.empty() && result.back().first == key) {
            // When key is duplicated, update value with the one from the latest SST
            if (sst_id > last_sst_id) {
                result.back().second = value;
            }
        } else {
            result.emplace_back(key, value);
        }
        last_sst_id = sst_id;

        // Update min-heap
        auto &sst = (*ssts)[sst_id];
        auto &page = current_pages[sst_id];
        if (page_index + 2 <= page.size()) {
            // In current page, read next index

            int64_t next_key = page[page_index++];
            int64_t next_value = page[page_index++];

            min_heap.push({next_key, next_value, page_index, sst_id});
        } else if (page_index + kPairSize > page.size()) {
            // Read next page
            offsets[sst_id] += kPageSize;
            if (offsets[sst_id] >= sst.file_size_) {
                LOG("      Read EOF " << sst.file_path_);
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

    return result;
}
