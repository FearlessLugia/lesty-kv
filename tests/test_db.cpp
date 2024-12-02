//
// Created by Kiiro Huang on 24-11-19.
//

#include <cassert>

#include "../include/database.h"
#include "../utils/log.h"
#include "test_base.h"

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
        assert(res[77].first == 1101);
        assert(res[77].second == 11010);
        assert(res.back().first == 4096);
        assert(res.back().second == 40960);
        LOG("==============================");

        db.Delete(1024);
        const optional<int64_t> value2 = db.Get(key);
        assert(!value2.has_value());

        db.Close();

        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestDbIntegrated, "TestDb::TestDbIntegrated");
        return result;
    }
};
