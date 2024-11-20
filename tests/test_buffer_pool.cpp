//
// Created by Kiiro Huang on 24-11-19.
//

#include "test_base.h"

class TestBufferPool : public TestBase {
    static bool TestInsert() { return true; }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestInsert, "TestBufferPool::TestInsert");
        return result;
    }
};
