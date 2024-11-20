#include <iostream>

#include "../utils/constants.h"
#include "database.h"
#include "memtable.h"
#include "../utils/log.h"

using namespace std;

int main() {
    Database db(kMemtableSize);

    db.Open("db1");
    // for (auto i = 1; i <= 5000; ++i) {
    //     db.put(i, i * 10);
    // }
    // db.close();
    //
    // db.open("db1");
    // for (auto i = 500; i <= 1000; ++i) {
    //     db.put(i, i * 100);
    // }
    // db.close();
    //
    // db.open("db1");
    // for (auto i = 400; i <= 600; ++i) {
    //     db.put(i, i * 1000);
    // }
    // db.close();

    // for (auto i = 1; i <= 1024; ++i) {
    //     db.put(i, -i * 10);
    // }


    constexpr int64_t key = 1024;
    const auto value = db.Get(key);
    LOG("Get result: ");
    if (value.has_value()) {
        LOG(" Get key " << key << " returns " << value.value());
    } else {
        LOG(" Key " << key << " not found");
    }

    const auto res = db.Scan(1024, 4096);
    // for (const auto &s: res) {
    //     LOG(s.first << ": " << s.second);
    // }
    // print the first and last pairs
    LOG("Scan result: ");
    LOG(" First pairs: " << res.front().first << ": " << res.front().second);
    LOG(" Last pairs: " << res.back().first << ": " << res.back().second);

    db.Close();
}
