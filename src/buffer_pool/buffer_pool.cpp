//
// Created by Kiiro Huang on 24-11-14.
//

#include "buffer_pool.h"
#include <iostream>
#include "../../external/MurmurHash3.h"
#include "../../utils/constants.h"
#include "../../utils/log.h"

BufferPool::BufferPool(const size_t capacity) : capacity_(capacity), size_(0) {
    buckets_ = new vector<BucketNode *>(capacity_);

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

Page *BufferPool::Get(const string &page_id) const {
    const auto page = FindPage(page_id);
    if (page) {
        LOG("  Page " << page_id << " hit in buffer pool");
        eviction_policy_->Update(page);
        return page;
    }
    LOG("    Page " << page_id << " does not hit in buffer pool");
    return nullptr;
}

Page *BufferPool::Put(const string &id, const vector<int64_t> &data) {
    if (Page *exist_page = FindPage(id)) {
        return exist_page;
    }

    // if buffer pool is at the threshold, apply eviction policy
    if (size_ >= capacity_ * kCoeffBufferPool) {
        Remove();
    }

    Page *new_page = new Page(id, data);

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
    if (!eviction_policy_->front_)
        return;

    // get the page to remove
    const Page *page_to_remove = eviction_policy_->front_->page_;
    // remove the page from the LRU queue
    eviction_policy_->Evict();
    LOG("    Removing page " << page_to_remove->id_ << " from buffer pool");

    // remove the page from the buffer pool
    const size_t index = HashFunction(page_to_remove->id_);
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

void BufferPool::RemoveLevel(int64_t level) {
    LOG("  Removing all pages for level: " << level);

    for (auto &bucket: *buckets_) {
        BucketNode *current = bucket;
        BucketNode *prev = nullptr;

        while (current) {
            Page *page = current->page_;

            if (page->eviction_policy_key_ == level) {
                eviction_policy_->EvictPage(page);

                if (prev) {
                    prev->next_ = current->next_;
                } else {
                    bucket = current->next_;
                }

                delete current->page_;
                BucketNode *temp = current;
                current = current->next_;
                delete temp;

                --size_;
            } else {
                prev = current;
                current = current->next_;
            }
        }
    }

    LOG("  Finished removing all pages for level: " << level);
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
