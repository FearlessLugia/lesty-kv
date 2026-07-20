//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef LRU_H
#define LRU_H

#include "../../../include/buffer_pool/eviction_policy.h"
#include "queue_node.h"


class LRU : public EvictionPolicy {
public:
    LRU();
    ~LRU() override;

    QueueNode *Put(int64_t key, Page *page) override;

    bool Update(QueueNode *node) override;

    Page *Evict() override;

    void Remove(QueueNode *node) override;

    void Clear() override;

    [[nodiscard]] bool IsEmpty() const { return dummy_->next_ == dummy_; }

    [[nodiscard]] QueueNode *GetFront() const { return IsEmpty() ? nullptr : dummy_->next_; }
    [[nodiscard]] QueueNode *GetRear() const { return IsEmpty() ? nullptr : dummy_->prev_; }

private:
    QueueNode *dummy_;

    void AddToTail(QueueNode *node) const;

    static void Unlink(const QueueNode *node);
};

#endif // LRU_H
