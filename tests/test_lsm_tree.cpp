//
// Created by Kiiro Huang on 2024-11-24.
//

#include <cassert>

#include "../src/database.h"
#include "../src/lsm_tree/lsm_tree.h"
#include "test_base.h"

class TestLsmTree : public TestBase {
    static bool TestMultipleMergeSort() {
        Database db(kMemtableSize);
        const string db_name = "db1";
        db.Open(db_name);

        const auto a = new BTreeSSTable("db1/btree3.bin", false);
        const auto b = new BTreeSSTable("db1/btree4.bin", false);
        const auto c = new BTreeSSTable("db1/btree5.bin", false);

        LsmTree lsm_tree = LsmTree();
        const auto result = lsm_tree.SortMerge(new vector{a, b, c});

        assert(result.size() == 2048);
        assert(result[0] == 1);
        assert(result[1] == -10);

        assert(result[998] == 500);
        assert(result[999] == -5000);

        assert(result[2046] == 1024);
        assert(result[2047] == -10240);

        // for (auto entry: result) {
        //     cout << entry << endl;
        // }

        delete a;
        delete b;
        delete c;

        return true;
    }

    static bool TestMultipleMergeSort2() {
        Database db(kMemtableSize);
        const string db_name = "db1";
        db.Open(db_name);

        const auto a = new BTreeSSTable("db1/btree0.bin", false);
        const auto b = new BTreeSSTable("db1/btree1.bin", false);
        const auto c = new BTreeSSTable("db1/btree2.bin", false);

        LsmTree lsm_tree = LsmTree();
        const auto result = lsm_tree.SortMerge(new vector{a, b, c});

        assert(result.size() == 10000);

        // for (auto &[frt, snd]: result) {
        //     cout << frt << " " << snd << endl;
        // }

        delete a;
        delete b;
        delete c;

        return true;
    }

    static bool TestMergeSortALsmTree() {
        Database db(kMemtableSize);
        const string db_name = "db1";
        db.Open(db_name);

        const auto a = new BTreeSSTable("db1/btree0.bin", false);
        const auto b = new BTreeSSTable("db1/btree1.bin", false);
        const auto c = new BTreeSSTable("db1/btree2.bin", false);
        vector input = {a, b, c};

        LsmTree lsm_tree = LsmTree();
        lsm_tree.level_ = 1;
        lsm_tree.levelled_sst_ = {input};
        lsm_tree.SortMergePreviousLevel();

        assert(lsm_tree.levelled_sst_.size() == 2);
        assert(lsm_tree.levelled_sst_[0].size() == 0);
        assert(lsm_tree.levelled_sst_[1].size() == 1);
        assert(lsm_tree.levelled_sst_[1][0]->max_key_ == 5000);


        // for (auto &[frt, snd]: result) {
        //     cout << frt << " " << snd << endl;
        // }

        delete a;
        delete b;
        delete c;

        return true;
    }

    static bool TestLsmTreeIntegrated() {
        Database db(kMemtableSize);
        const string db_name = "db1";

        db.Open(db_name);
        for (auto i = 1; i <= 5000; ++i) {
            db.Put(i, i * 10);
        }
        db.Close();

        db.Open(db_name);
        for (auto i = 500; i <= 1000; ++i) {
            db.Put(i, i * 100);
        }
        db.Close();

        db.Open(db_name);
        for (auto i = 400; i <= 600; ++i) {
            db.Put(i, i * 1000);
        }
        db.Close();

        db.Open(db_name);
        for (auto i = 1; i <= 1024; ++i) {
            db.Put(i, -i * 10);
        }
        db.Close();

        db.Open(db_name);
        for (auto i = 900; i <= 1100; ++i) {
            db.Put(i, -i * 100);
        }
        db.Close();

        db.Open(db_name);




        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        // result &= AssertTrue(TestMultipleMergeSort, "TestLsmTree::TestMultipleMergeSort");
        // result &= AssertTrue(TestMultipleMergeSort2, "TestLsmTree::TestMultipleMergeSort2");
        // result &= AssertTrue(TestMergeSortALsmTree, "TestLsmTree::TestMergeSortALsmTree");
        result &= AssertTrue(TestLsmTreeIntegrated, "TestLsmTree::TestLsmTreeIntegrated");
        return result;
    }
};
