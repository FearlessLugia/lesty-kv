//
// Created by Kiiro Huang on 24-11-14.
//

#include "../../include/buffer_pool/buffer_pool.h"

#include <iostream>

#include "../../external/MurmurHash3.h"
#include "../../utils/constants.h"
#include "../../utils/log.h"

using namespace std;

BufferPool::BufferPool(const size_t capacity) : capacity_(capacity), size_(0) {
    buckets_ = new vector<BucketNode *>(capacity_);

    eviction_policy_ = new LRU();
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
    MurmurHash3_x64_128(key.c_str(), key.size(), 17, hash);
    return hash[0] % buckets_->size();
}

Page *BufferPool::FindPage(const string &id) const {
    const size_t index = HashFunction(id);

    const BucketNode *current = (*buckets_)[index];
    while (current) {
        if (current->page_->GetId() == id) {
            return current->page_;
        }
        current = current->next_;
    }

    return nullptr;
}

Page *BufferPool::Get(const string &page_id) const {
    const size_t index = HashFunction(page_id);
    BucketNode *current = (*buckets_)[index];
    while (current) {
        if (current->page_->GetId() == page_id) {
            LOG("  Page " << page_id << " hit in buffer pool");
            eviction_policy_->Update(current->lru_node_);
            return current->page_;
        }
        current = current->next_;
    }
    LOG("    Page " << page_id << " does not hit in buffer pool");
    return nullptr;
}

Page *BufferPool::Put(const string &id, vector<int64_t> data) {
    if (Page *exist_page = FindPage(id)) {
        return exist_page;
    }

    // if buffer pool is at the threshold, apply eviction policy
    if (size_ >= capacity_ * kCoeffBufferPool) {
        Remove();
    }

    Page *new_page = new Page(id, std::move(data));

    const size_t index = HashFunction(id);
    BucketNode *new_node = new BucketNode(new_page);
    new_node->next_ = (*buckets_)[index];
    (*buckets_)[index] = new_node;

    ++size_;

    // maintain the LRU queue and record lru_node_ in BucketNode
    new_node->lru_node_ = eviction_policy_->Put(index, new_page);

    return new_page;
}

void BufferPool::Remove() {
    // Evict least recently used page from LRU queue
    const Page *page_to_remove = eviction_policy_->Evict();
    if (!page_to_remove)
        return;

    LOG("    Removing page " << page_to_remove->GetId() << " from buffer pool");

    // remove the page from the buffer pool
    const size_t index = HashFunction(page_to_remove->GetId());
    BucketNode *current = (*buckets_)[index];
    BucketNode *prev = nullptr;

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

    eviction_policy_->Clear();

    LOG("  Buffer pool cleared");
}

void BufferPool::Resize() {}
