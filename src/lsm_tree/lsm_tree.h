//
// Created by Kiiro Huang on 2024-11-24.
//

#ifndef LSM_TREE_H
#define LSM_TREE_H
#include "../b_tree/b_tree_sstable.h"


class LsmTree {

public:
    int64_t level_;
    vector<vector<BTreeSSTable>> levelled_sst_;

    vector<pair<int64_t, int64_t>> SortMerge(vector<BTreeSSTable> *ssts);

    void SortMergePreviousLevel();

    void SortMergeLastLevel();

    void FlushToNewLevel();
};


#endif // LSM_TREE_H
