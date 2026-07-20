//
// Created by Kiiro Huang on 24-11-19.
//

#include <cassert>
#include <iostream>

#include "../include/buffer_pool/buffer_pool.h"
#include "test_base.h"

using namespace std;

class TestBufferPool : public TestBase {
    static bool TestBuckets() {
        BufferPool bufferPool(4);

        const string page1_id = "test1_128";
        const string page2_id = "test2_128";
        const string page3_id = "test1_256";
        auto page1_data = vector<int64_t>(1, 1);
        auto page2_data = vector<int64_t>(2, 2);
        auto page3_data = vector<int64_t>(3, 3);

        bufferPool.Put(page1_id, page1_data);
        bufferPool.Put(page2_id, page2_data);
        bufferPool.Put(page3_id, page3_data);

        // Check if the data are in the same address
        assert(bufferPool.Get("test1_128")->data_ == page1_data);
        assert(bufferPool.Get("test2_128")->data_ == page2_data);
        assert(bufferPool.Get("test1_256")->data_ == page3_data);

        return true;
    }

    static bool TestLRU() {
        BufferPool bufferPool(4);

        const string page1_id = "test1_128";
        const string page2_id = "test2_128";
        const string page3_id = "test1_256";
        auto page1_data = vector<int64_t>(1, 1);
        auto page2_data = vector<int64_t>(2, 2);
        auto page3_data = vector<int64_t>(3, 3);

        bufferPool.Put(page1_id, page1_data);
        bufferPool.Put(page2_id, page2_data);
        bufferPool.Put(page3_id, page3_data);

        const Page *page2 = bufferPool.Get(page2_id);
        (void)bufferPool.Get(page1_id); // suppression for nodiscard
        const Page *page3 = bufferPool.Get(page3_id);

        // Check if the pages in the LRU queue are in the same address as the pages in the buffer pool
        // Check if the pages in the LRU queue are in the correct order
        // page3 should be the most recent (so in the back)
        assert(bufferPool.eviction_policy_->GetFront()->page_ == page2);
        assert(bufferPool.eviction_policy_->GetRear()->page_ == page3);

        return true;
    }

    static bool TestLRUEvict() {
        BufferPool bufferPool(6);

        for (int i = 0; i < 6; i++) {
            bufferPool.Put("test" + to_string(i), vector<int64_t>(i, i));
        }

        const Page *page = bufferPool.Get("test4");

        // Check if the pages in the LRU queue are in the correct order, page of name test4 should be the most recent
        assert(bufferPool.eviction_policy_->GetRear()->page_ == page);

        return true;
    }

    static bool TestBucketNodePointers() {
        BufferPool bufferPool(4);

        const string p1 = "p1";
        const string p2 = "p2";
        const string p3 = "p3";

        bufferPool.Put(p1, {1});
        bufferPool.Put(p2, {2});
        bufferPool.Put(p3, {3});

        // Verify order in LRU: front should be p1, rear should be p3
        assert(bufferPool.eviction_policy_->GetFront()->page_->id_ == p1);
        assert(bufferPool.eviction_policy_->GetRear()->page_->id_ == p3);

        // Get p1 -> should move p1 to rear in O(1)
        (void)bufferPool.Get(p1); // suppression for nodiscard
        assert(bufferPool.eviction_policy_->GetFront()->page_->id_ == p2);
        assert(bufferPool.eviction_policy_->GetRear()->page_->id_ == p1);
        return true;
    }

public:
    bool RunTests() override {
        bool result = true;
        result &= AssertTrue(TestBuckets, "TestBufferPool::TestBuckets");
        result &= AssertTrue(TestLRU, "TestBufferPool::TestLRU");
        result &= AssertTrue(TestLRUEvict, "TestBufferPool::TestLRUEvict");
        result &= AssertTrue(TestBucketNodePointers, "TestBufferPool::TestBucketNodePointers");
        return result;
    }
};
