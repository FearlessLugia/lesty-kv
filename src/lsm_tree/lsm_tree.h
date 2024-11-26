//
// Created by Kiiro Huang on 2024-11-24.
//

#ifndef LSM_TREE_H
#define LSM_TREE_H
#include "../b_tree/b_tree_sstable.h"


class LsmTree {

public:
    vector<pair<int64_t, int64_t>> SortMerge(vector<BTreeSSTable> *ssts);
};


#endif // LSM_TREE_H
