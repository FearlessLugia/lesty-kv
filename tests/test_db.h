//
// Created by Kiiro Huang on 24-11-19.
//

#include <cassert>

#include "../include/database.h"
#include "../utils/log.h"
#include "test_base.h"

using namespace std;

class TestDb : public TestBase {
    static bool TestDbIntegrated() {
        Database db(32 * 1024); // 32KB
        const string db_name = "test_db";
        filesystem::remove_all(db_name);

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

        constexpr int64_t key = 1024;
        const optional<int64_t> value = db.Get(key);
        assert(value.has_value() && value.value() == -102400);
        LOG("==============================");

        const auto res = db.Scan(1024, 4096);
        assert(res.front().first == 1024);
        assert(res.front().second == -102400);
        bool found_1101 = false;
        for (const auto& [k, v] : res) {
            if (k == 1101) {
                assert(v == 11010);
                found_1101 = true;
                break;
            }
        }
        assert(found_1101);
        assert(res.back().first == 4096);
        assert(res.back().second == 40960);
        LOG("==============================");

        db.Delete(1024);
        const optional<int64_t> value2 = db.Get(key);
        assert(!value2.has_value());

        db.Close();

        return true;
    }

    static bool TestDbEdgeCases() {
        Database db(32 * 1024); // 32KB
        const string db_name = "test_db_edge";
        filesystem::remove_all(db_name);

        db.Open(db_name);

        LOG("--- Edge Case 1: Overwrite ---");
        db.Put(888, 100);
        db.FlushFromMemtable(); // Force to SSTable
        db.Put(888, 200);
        assert(db.Get(888).value() == 200);

        // Scan should also get the latest value
        auto res = db.Scan(800, 900);
        bool found_888 = false;
        for (const auto& [k, v] : res) {
            if (k == 888) {
                assert(v == 200);
                found_888 = true;
                break;
            }
        }
        assert(found_888);

        LOG("--- Edge Case 2: Tombstone Scan (Bug #3) ---");
        db.Put(999, 9990);
        db.FlushFromMemtable();
        db.Delete(999); // Writes INT64_MIN
        
        res = db.Scan(900, 1000);
        for (const auto& [k, v] : res) {
            assert(k != 999); // 999 should not appear in scan!
        }

        LOG("--- Edge Case 3: Boundary Checks ---");
        assert(!db.Get(10).has_value()); // Smaller than all keys
        assert(!db.Get(99999).has_value()); // Larger than all keys

        LOG("--- Edge Case 4: Sequential Flooding (Bug #5) ---");
        // Insert 20,000 keys to exceed the 64 page (16,384 keys) threshold
        for (int i = 10000; i < 30000; ++i) {
            db.Put(i, i * 2);
        }
        
        // Scan 25,000 keys (10000 to 35000), which will definitely trigger sequential flooding.
        res = db.Scan(10000, 35000);
        
        // Verify we got exactly the 20,000 inserted keys
        if (res.size() != 20000) {
            std::cout << "ACTUAL RES SIZE: " << res.size() << std::endl;
            assert(res.size() == 20000);
        }
        assert(res.front().first == 10000);
        assert(res.back().first == 29999);

        db.Close();
        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestDbIntegrated, "TestDb::TestDbIntegrated");
        result &= AssertTrue(TestDbEdgeCases, "TestDb::TestDbEdgeCases");
        return result;
    }
};
