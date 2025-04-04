//
// Created by Kiiro Huang on 24-11-21.
//

#ifndef B_TREE_H
#define B_TREE_H
#include <sys/_types/_off_t.h>
#include "../buffer_pool/page.h"
#include "../memtable.h"
#include "../sstable.h"

class BTreeSSTable : public SSTable {
public:
    vector<int64_t> root_;
    vector<vector<int64_t>> internal_nodes_;

    // Default level set to 0, as it is the first level of the B-Tree
    BTreeSSTable(const string &db_name, bool create_new, int64_t level = 0);

    void WritePage(const off_t offset, const Page *page, bool is_final_page) const;

    string FlushToStorage(const vector<int64_t> *data);

    void GenerateBTreeLayers(vector<int64_t> prev_layer_nodes);
    off_t ReadOffset() const;

private:
    void InitialKeyRange() override;

    // Returns the offset of startKey or nullopt if not found
    optional<int64_t> BinarySearch(const int64_t key) const override;

    // Returns the offset of startKey or its upper bound if not found
    int64_t BinarySearchUpperbound(const int64_t key, bool is_sequential_flooding) const override;

    vector<pair<int64_t, int64_t>> LinearSearchToEndKey(off_t start_offset, int64_t start_key, int64_t end_key, bool is_sequential_flooding) const override;
};


#endif // B_TREE_H
