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
        Database db(kMemtableSize);
        const string db_name = "db1";
        db.Open(db_name);

        // auto b_tree_ssts = db.GetSortedBTreeSsts("db_name");

        // for (const auto &b_tree_sst_path: b_tree_ssts) {
        //     LOG(" Get in " << b_tree_sst_path.string());
        //
        //     BufferPool *buffer_pool = new BufferPool(kBufferPoolSize);
        //
        //     auto b_tree_sst = BTreeSst(b_tree_sst_path, buffer_pool);
        //     // auto sst_value = b_tree_sst.WritePage();
        // }


        db.Close();
        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestBuildBTree, "TestBTree::TestBuildBTree");
        return result;
    }
};
