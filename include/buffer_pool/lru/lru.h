//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef LRU_H
#define LRU_H
#include <cstdint>
#include <list>

#include "../../../include/buffer_pool/eviction_policy.h"
#include "queue_node.h"

using namespace std;


class LRU : public EvictionPolicy {
public:
    QueueNode *front_;
    QueueNode *rear_;

    size_t capacity_;
    size_t size_ = 0;

    explicit LRU(size_t capacity);
    ~LRU();

    void MoveToTail(QueueNode *node);

    bool Update(Page *page);

    void Put(int64_t key, Page *page) override;

    void Evict() override;

    void EvictPage(Page *page) override;

    void Clear();
};


#endif // LRU_H
