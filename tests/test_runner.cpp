//
// Created by Kiiro Huang on 24-11-19.
//

#include <iostream>

#include "test_b_tree.cpp"
#include "test_base.h"
#include "test_buffer_pool.cpp"
#include "test_db.cpp"

using namespace std;

int main() {
    vector<std::pair<TestBase *, string>> testClasses = {
            // make_pair(new TestBufferPool(), "TestBufferPool"),
            // make_pair(new TestBTree(), "TestBTree"),
            make_pair(new TestDb(), "TestDb"),
    };

    bool allTestPassed = true;
    cout << endl << "Running tests..." << endl;
    for (auto [testClass, name]: testClasses) {
        cout << "Running " << name << endl;
        allTestPassed &= testClass->RunTests();
        cout << "==============================" << endl;
    }

    if (allTestPassed) {
        cout << "All Tests Passed." << endl;
    }
}
