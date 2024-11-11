#include <iostream>

#include "constants.h"
#include "database.h"
#include "memtable.h"

using namespace std;

int main() {
    Database db(kMemtableSize);;
    db.open("db1");

    // for (auto i = 1; i <= 500; ++i) {
    //     db.put(i, i * 100);
    // }

    constexpr int64_t key = 51234;
    const auto value = db.get(key);
    cout << "Get result: " << endl;
    if (value.has_value()) {
        cout << " Get key " << key << " returns " << value.value() << endl;
    } else {
        cout << " Key " << key << " not found" << endl;
    }

    const auto res = db.scan(300, 800);
    // print the first and last pairs
    cout << "Scan result: " << endl;
    cout << " First pairs: " << res.front().first << ": " << res.front().second << endl;
    cout << " Last pairs: " << res.back().first << ": " << res.back().second << endl;

    db.close();
}
