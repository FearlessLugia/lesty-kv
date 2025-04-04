//
// Created by Kiiro Huang on 24-11-22.
//


#include <cassert>
#include <iostream>

#include "../include/b_tree/b_tree_sstable.h"
#include "../include/database.h"
#include "test_base.h"

class TestBTree : public TestBase {
    static bool TestBuildBTree() {
        Database db(32 * 1024); // 32KB
        const string db_name = "test_db";
        filesystem::remove_all(db_name);

        db.Open(db_name);

        const auto btree = new BTreeSSTable(db_name, true);

        vector<int64_t> data;
        for (auto i = 1; i <= 2048; ++i) {
            data.push_back(i);
            data.push_back(i * 100);
        }
        btree->FlushToStorage(&data);

        assert(btree->root_.size() == 1);
        assert(btree->root_[0] == 2048);
        assert(btree->internal_nodes_.size() == 1);
        assert(btree->internal_nodes_[0] == vector<int64_t>({256, 512, 768, 1024, 1280, 1536, 1792, 2048}));

        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestBuildBTree, "TestBTree::TestBuildBTree");
        return result;
    }
};
