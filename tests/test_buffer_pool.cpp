//
// Created by Kiiro Huang on 24-11-19.
//

#include <cassert>


#include "../src/buffer_pool/buffer_pool.h"
#include "test_base.h"

class TestBufferPool : public TestBase {
    static bool TestInsert() {
        BufferPool *bufferPool = new BufferPool(4);
        bufferPool->buckets_ = new vector<forward_list<Page>>(2);

        Page *page1 = new Page("test1_128");
        page1->data_= new vector<int64_t>(3, 1);
        Page *page2 = new Page("test2_128");
        page2->data_= new vector<int64_t>(3, 2);
        Page *page3 = new Page("test1_256");
        page3->data_= new vector<int64_t>(3, 3);

        bufferPool->Put(page1->id_, *page1->data_);
        bufferPool->Put(page2->id_, *page2->data_);
        bufferPool->Put(page3->id_, *page3->data_);

        Page *page1_128 = bufferPool->Get("test1_128");
        Page *page2_128 = bufferPool->Get("test2_128");
        Page *page1_256 = bufferPool->Get("test1_256");

        assert(*page1->data_==*page1_128->data_);
        assert(*page2->data_==*page2_128->data_);
        assert(*page3->data_==*page1_256->data_);

        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestInsert, "TestBufferPool::TestInsert");
        return result;
    }
};
