//
// Created by Kiiro Huang on 24-11-19.
//

#ifndef TESTBASE_H
#define TESTBASE_H

#include <iostream>

using namespace std;

class TestBase {
protected:
    static bool AssertTrue(const function<bool()> &test, const string &name) {
        cout << "  - " << name << " - ";
        if (!(invoke(test))) {
            cout << "failed ❌" << endl;
            return false;
        }
        cout << "passed ✅" << endl;
        return true;
    }

public:
    TestBase() = default;
    virtual bool RunTests() = 0;
};


#endif // TESTBASE_H
