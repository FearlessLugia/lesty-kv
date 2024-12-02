//
// Created by Kiiro Huang on 24-11-19.
//

#include <cassert>

#include "../src/database.h"
#include "../utils/constants.h"
#include "../utils/log.h"
#include "test_base.h"

class TestDb : public TestBase {
    static bool TestDbIntegrated() {
        Database db(kMemtableSize);
        const string db_name = "db1";

        db.Open(db_name);
        for (auto i = 1; i <= 5000; ++i) {
            db.Put(i, i * 10);
        }
        db.Close();

        // db.Open(db_name);
        // for (auto i = 500; i <= 1000; ++i) {
        //     db.Put(i, i * 100);
        // }
        // db.Close();
        //
        // db.Open(db_name);
        // for (auto i = 400; i <= 600; ++i) {
        //     db.Put(i, i * 1000);
        // }
        // db.Close();
        //
        // db.Open(db_name);
        // for (auto i = 1; i <= 1024; ++i) {
        //     db.Put(i, -i * 10);
        // }
        // db.Close();
        //
        // db.Open(db_name);
        // for (auto i = 900; i <= 1100; ++i) {
        //     db.Put(i, -i * 100);
        // }
        // db.Close();

        db.Open(db_name);

        constexpr int64_t key = 1024;
        const optional<int64_t> value = db.Get(key);
        cout << "Get result: " << endl;
        if (value.has_value()) {
            cout << " Get key " << key << " returns " << value.value() << endl;
        } else {
            cout << " Key " << key << " not found" << endl;
        }
        assert(value.has_value() && value.value() == -10240);
        cout << "==============================" << endl;

        const auto res = db.Scan(1024, 4096);
        // for (const auto &s: res) {
        //     LOG(s.first << ": " << s.second);
        // }
        // Print the first and last pairs
        cout << "Scan result: " << endl;
        // cout << " First pairs: " << res.front().first << ": " << res.front().second << endl;
        // cout << " Last pairs: " << res.back().first << ": " << res.back().second << endl;
        assert(res.front().first == 1024);
        assert(res.front().second == -10240);
        assert(res[1].first == 1025);
        assert(res[1].second == 10250);
        assert(res.back().first == 4096);
        assert(res.back().second == 40960);
        cout << "==============================" << endl;

        db.Delete(1024);
        const optional<int64_t> value2 = db.Get(key);
        cout << "Get result: " << endl;
        if (value2.has_value()) {
            cout << " Get key " << key << " returns " << value2.value() << endl;
        } else {
            cout << " Key " << key << " not found" << endl;
        }
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
