//
// Created by Kiiro Huang on 24-11-22.
//


#include <cassert>


#include <iostream>
#include "../src/b_tree/b_tree_sstable.h"
#include "../src/buffer_pool/buffer_pool.h"
#include "../src/database.h"
#include "../utils/log.h"
#include "test_base.h"

class TestBTree : public TestBase {
    static bool TestBuildBTree() {
        BTreeSSTable btree("db1", true);

        vector<int64_t> data;
        for (auto i = 1; i <= 2048; ++i) {
            data.push_back(i);
            data.push_back(i * 100);
        }

        btree.FlushToStorage(&data);

        assert(btree.root_.size() == 1);
        assert(btree.root_[0] == 1024);
        assert(btree.internal_nodes_.size() == 2);
        assert(btree.internal_nodes_[0] == vector<int64_t>({256, 512, 768}));
        assert(btree.internal_nodes_[1] == vector<int64_t>({1280, 1536, 1792}));

        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestBuildBTree, "TestBTree::TestBuildBTree");
        return result;
    }
};
