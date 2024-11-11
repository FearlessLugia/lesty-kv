#include <iostream>

#include "constants.h"
#include "database.h"
#include "memtable.h"

using namespace std;

int main() {
    Database db(kMemtableSize);;
    db.open("db1");

    // for (auto i=1;i<=2048;++i) {
    //     db.put(i, i*10);
    // }

    constexpr int64_t key = 1000;
    const auto value = db.get(key);
    if (value.has_value()) {
        cout << "Get key " << key << " returns " << value.value() << endl;
    } else {
        cout << "Key " << key << " not found" << endl;
    }

    // cout << "Scan result: " << endl;
    // const auto res = table.Scan(1, 3);
    // for (const auto &s: res) {
    //     cout << s.first << ": " << s.second << endl;
    // }

    db.close();
}
