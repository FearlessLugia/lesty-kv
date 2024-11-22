//
// Created by Kiiro Huang on 24-11-14.
//

#include "buffer_pool.h"
#include <iostream>
#include "../../external/MurmurHash3.h"
#include "../../utils/constants.h"
#include "../../utils/log.h"

BufferPool::BufferPool(const size_t capacity) : capacity_(capacity), size_(0) {
    buckets_ = new vector<BucketNode*>(capacity_);

    // set the LRU queue size to that of the buffer pool - 1
    eviction_policy_ = new LRU(capacity_ - 1);
}

BufferPool::~BufferPool() {
    for (auto &head: *buckets_) {
        while (head) {
            const BucketNode *temp = head;
            head = head->next_;
            delete temp->page_;
            delete temp;
        }
    }

    delete buckets_;
}


size_t BufferPool::HashFunction(const string &key) const {
    uint64_t hash[2];
    MurmurHash3_x64_128(key.c_str(), sizeof(&key), 17, hash);
    return hash[0] % buckets_->size();
}

Page *BufferPool::FindPage(const string &id) const {
    const size_t index = HashFunction(id);

    const BucketNode *current = (*buckets_)[index];
    while (current) {
        if (current->page_->id_ == id) {
            return current->page_;
        }
        current = current->next_;
    }

    return nullptr;
}

Page *BufferPool::Get(const string &page_id) {
    auto page = FindPage(page_id);
    if (page) {
        LOG("hit in buffer pool");
        eviction_policy_->Update(page);
        return page;
    }
    LOG("do not hit in buffer pool");

    auto new_page = Put(page_id, *page->data_);
    return new_page;
}

Page *BufferPool::Put(const string &id, const vector<int64_t> &data) {
    if (Page *exist_page = FindPage(id)) {
        exist_page->data_ = &data;
        return exist_page;
    }

    // if buffer pool is at the threshold, apply eviction policy
    if (size_ >= capacity_ * kCoeffBufferPool) {
        Remove();
    }

    Page *new_page = new Page(id);
    new_page->data_ = &data;

    const size_t index = HashFunction(id);
    BucketNode *new_node = new BucketNode(new_page);
    new_node->next_ = (*buckets_)[index];
    (*buckets_)[index] = new_node;

    ++size_;

    // maintain the LRU queue
    eviction_policy_->Put(index, new_page);

    return new_page;
}

void BufferPool::Remove() {
    // if the LRU queue is empty, return
    if (!eviction_policy_->front_) return;

    // get the page to remove
    Page* page_to_remove = eviction_policy_->front_->page_;
    // remove the page from the LRU queue
    eviction_policy_->Evict();

    // remove the page from the buffer pool
    const size_t index = HashFunction(page_to_remove->id_);
    BucketNode* current = (*buckets_)[index];
    BucketNode* prev = nullptr;

    while (current) {
        if (current->page_ == page_to_remove) {
            if (prev) {
                prev->next_ = current->next_;
            } else {
                (*buckets_)[index] = current->next_;
            }
            delete current->page_;
            delete current;
            --size_;
            return;
        }
        prev = current;
        current = current->next_;
    }
}

void BufferPool::Clear() {
    for (auto &head: *buckets_) {
        while (head) {
            const BucketNode *temp = head;
            head = head->next_;
            delete temp->page_;
            delete temp;
        }
        head = nullptr;
    }
    size_ = 0;
}

void BufferPool::Resize() {}
