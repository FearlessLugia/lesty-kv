//
// Created by Kiiro Huang on 2024-11-24.
//

#ifndef LSM_TREE_H
#define LSM_TREE_H
#include "../b_tree/b_tree_sstable.h"


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
    LsmTree();
    ~LsmTree();

    LsmTree(const LsmTree &) = delete;
    LsmTree &operator=(const LsmTree &) = delete;

public:
    vector<vector<BTreeSSTable *>> levelled_sst_;

    static LsmTree &GetInstance();

    vector<int64_t> SortMerge(vector<BTreeSSTable *> *ssts, bool should_dispose_tombstone);
    void AddSst(BTreeSSTable *sst);

    void SortMergePreviousLevel(int64_t current_level);
    void DeleteFile(BTreeSSTable *sst);

    void SortMergeLastLevel();

    vector<vector<BTreeSSTable *>> ReadSSTsFromStorage();

    void BuildLsmTree();

    void OrderLsmTree();
};


#endif // LSM_TREE_H
