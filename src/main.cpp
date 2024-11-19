#include <iostream>

#include "../utils/constants.h"
#include "database.h"
#include "memtable.h"

using namespace std;

int main() {
    Database db(kMemtableSize);
    ;

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
    cout << "Get result: " << endl;
    if (value.has_value()) {
        cout << " Get key " << key << " returns " << value.value() << endl;
    } else {
        cout << " Key " << key << " not found" << endl;
    }

    const auto res = db.Scan(1024, 4096);
    // for (const auto &s: res) {
    //     cout << s.first << ": " << s.second << endl;
    // }
    // print the first and last pairs
    cout << "Scan result: " << endl;
    cout << " First pairs: " << res.front().first << ": " << res.front().second << endl;
    cout << " Last pairs: " << res.back().first << ": " << res.back().second << endl;

    db.Close();
}
