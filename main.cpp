#include <iostream>

#include "memtable.h"

using namespace std;

int main() {
    Memtable table;

    table.Put(1, 100);
    table.Put(2, 200);
    table.Put(3, 300);

    const auto value = table.Get(2);
    if (value.has_value()) {
        cout << value.value() << endl;
    } else {
        cout << "Not found" << endl;
    }

    const auto res = table.Scan(1, 3);
    for (const auto &s: res) {
        cout << s.first << ": " << s.second << endl;
    }
}
