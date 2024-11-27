//
// Created by Kiiro Huang on 2024-11-24.
//

#include <cassert>

#include "../src/database.h"
#include "../src/lsm_tree/lsm_tree.h"
#include "test_base.h"

class TestLsmTree : public TestBase {
    static bool TestMultipleMergeSort() {
        BTreeSSTable a = BTreeSSTable("db1/btree3.bin", false);
        BTreeSSTable b = BTreeSSTable("db1/btree4.bin", false);
        BTreeSSTable c = BTreeSSTable("db1/btree5.bin", false);
        vector input = {a, b, c};

        LsmTree lsm_tree = LsmTree();
        const auto result = lsm_tree.SortMerge(&input);

        assert(result.size() == 1024);
        assert(result[0] == make_pair(1, -10));
        assert(result[499] == make_pair(500, -5000));
        assert(result[1023] == make_pair(1024, -10240));

        // for (auto &[frt, snd]: result) {
        //     cout << frt << " " << snd << endl;
        // }

        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestMultipleMergeSort, "TestLsmTree::TestMultipleMergeSort");
        return result;
    }
};
