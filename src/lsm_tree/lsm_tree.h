//
// Created by Kiiro Huang on 2024-11-24.
//

#ifndef LSM_TREE_H
#define LSM_TREE_H
#include "../b_tree/b_tree_sstable.h"


class LsmTree {

public:
    int64_t level_;
    vector<vector<BTreeSSTable *>> levelled_sst_;

    vector<int64_t> SortMerge(vector<BTreeSSTable *> *ssts);

    void SortMergePreviousLevel();
    void DeleteFile(const std::string &file_path);

    void SortMergeLastLevel();

    void SortMergeWritePreviousLevel(vector<BTreeSSTable *> *nodes, int current_level);

    void SortMergeWriteLastLevel(vector<BTreeSSTable *> *nodes);

    vector<vector<BTreeSSTable>> ReadSSTsFromStorage();

    void BuildLsmTree();
};


#endif // LSM_TREE_H
