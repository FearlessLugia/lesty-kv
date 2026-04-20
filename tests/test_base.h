//
// Created by Kiiro Huang on 24-11-19.
//

#ifndef TESTBASE_H
#define TESTBASE_H

#include <functional>
#include <iostream>
#include <string>

class TestBase {
protected:
    static bool AssertTrue(const std::function<bool()> &test, const std::string &name) {
        std::cout << "  - " << name << " - ";
        if (!(std::invoke(test))) {
            std::cout << "failed ❌" << std::endl;
            return false;
        }
        std::cout << "passed ✅" << std::endl;
        return true;
    }

public:
    TestBase() = default;
    virtual bool RunTests() = 0;
};


#endif // TESTBASE_H
