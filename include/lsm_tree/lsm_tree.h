//
// Created by Kiiro Huang on 2024-11-24.
//

#ifndef LSM_TREE_H
#define LSM_TREE_H
#include <vector>
#include "../../include/b_tree/b_tree_sstable.h"
#include "../../include/buffer_pool/buffer_pool.h"
#include "../../include/sst_counter.h"


struct HeapNode {
    int64_t key;
    int64_t value;
    size_t page_index; // index of int64_t inside the current page
    size_t sst_id;

    bool operator>(const HeapNode &other) const {
        // Smaller Key has higher priority
        if (key != other.key) {
            return key > other.key;
        }

        // When same key, larger sst_id has higher priority
        return sst_id < other.sst_id;
    }
};

class LsmTree {
    BufferPool *buffer_pool_;
    SSTCounter *sst_counter_;

public:
    LsmTree(BufferPool *buffer_pool, SSTCounter *sst_counter);
    ~LsmTree();

    LsmTree(const LsmTree &) = delete;
    LsmTree &operator=(const LsmTree &) = delete;
    std::vector<std::vector<BTreeSSTable *>> levelled_sst_;

    std::vector<int64_t> SortMerge(std::vector<BTreeSSTable *> *ssts, bool should_dispose_tombstone);
    void AddSst(BTreeSSTable *sst);

    void SortMergePreviousLevel(int64_t current_level);
    static void DeleteFile(BTreeSSTable *sst);

    void SortMergeLastLevel();

    std::vector<std::vector<BTreeSSTable *>> ReadSSTsFromStorage();

    void BuildLsmTree();

    void OrderLsmTree();
};


#endif // LSM_TREE_H
