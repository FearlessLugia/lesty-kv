//
// Created by Kiiro Huang on 2024-11-24.
//

#include <cassert>

#include "../include/database.h"
#include "../include/lsm_tree/lsm_tree.h"
#include "test_base.h"

using namespace std;

class TestLsmTree : public TestBase {
    static bool TestMultipleMergeSort() {
        Database db(32 * 1024); // 32KB
        const string db_name = "test_db";
        filesystem::remove_all(db_name);

        db.Open(db_name);

        const auto a = new BTreeSSTable(db_name, true, db.GetBufferPool(), db.GetSSTCounter());
        const auto b = new BTreeSSTable(db_name, true, db.GetBufferPool(), db.GetSSTCounter());
        const auto c = new BTreeSSTable(db_name, true, db.GetBufferPool(), db.GetSSTCounter());

        vector<pair<int64_t, int64_t>> data;
        for (auto i = 1; i <= 5000; i++) {
            data.emplace_back(i, i * 10);
        }
        a->FlushToStorage(&data);

        data.clear();
        for (auto i = 400; i <= 600; i++) {
            data.emplace_back(i, i * 100);
        }
        b->FlushToStorage(&data);

        data.clear();
        for (auto i = 500; i <= 1000; i++) {
            data.emplace_back(i, -i);
        }
        c->FlushToStorage(&data);

        auto *lsm_tree = db.GetLsmTree();
        const auto result = lsm_tree->SortMerge(new vector{a, b, c}, true);

        assert(result.size() == 5000);
        assert(result[0].first == 1);
        assert(result[0].second == 10);

        assert(result[399].first == 400);
        assert(result[399].second == 40000);

        assert(result[499].first == 500);
        assert(result[499].second == -500);

        assert(result[4999].first == 5000);
        assert(result[4999].second == 50000);

        delete a;
        delete b;
        delete c;

        return true;
    }

    static bool TestBuildLsmTree() {
        Database db(32 * 1024);
        const string db_name = "test_db";
        filesystem::remove_all(db_name);

        db.Open(db_name);
        for (auto i = 1; i <= 5000; ++i) {
            db.Put(i, i * 10);
        }
        db.Close();

        db.Open(db_name);

        LsmTree *lsm_tree = db.GetLsmTree();
        assert(lsm_tree->levelled_sst_.size() == 2);
        assert(lsm_tree->levelled_sst_[0].size() == 0);
        assert(lsm_tree->levelled_sst_[1].size() == 1);
        assert(lsm_tree->levelled_sst_[1][0]->max_key_ == 5000);

        return true;
    }

    static bool TestLsmTreeIntegrated() {
        Database db(32 * 1024);
        const string db_name = "test_db1";
        filesystem::remove_all(db_name);

        db.Open(db_name);
        for (auto i = 1; i <= 5000; ++i) {
            db.Put(i, i * 10);
        }
        db.Close();

        db.Open(db_name);

        constexpr int64_t key = 1024;
        const optional<int64_t> value = db.Get(key);
        assert(value.has_value() && value.value() == 10240);

        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestMultipleMergeSort, "TestLsmTree::TestMultipleMergeSort");
        result &= AssertTrue(TestBuildLsmTree, "TestLsmTree::TestBuildLsmTree");
        result &= AssertTrue(TestLsmTreeIntegrated, "TestLsmTree::TestLsmTreeIntegrated");
        return result;
    }
};
