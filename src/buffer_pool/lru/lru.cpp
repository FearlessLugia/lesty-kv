//
// Created by Kiiro Huang on 24-11-20.
//

#include "../../../include/buffer_pool/lru/lru.h"

#include "../../../utils/log.h"

#include "../../../include/buffer_pool/bucket_node.h"

LRU::LRU() {
    dummy_ = new QueueNode(-1, nullptr);
    dummy_->next_ = dummy_;
    dummy_->prev_ = dummy_;
}

LRU::~LRU() {
    LRU::Clear();

    delete dummy_;
}

void LRU::AddToTail(QueueNode *node) const {
    QueueNode *last = dummy_->prev_;
    last->next_ = node;
    node->prev_ = last;
    
    node->next_ = dummy_;
    dummy_->prev_ = node;
}

void LRU::Unlink(const QueueNode *node) {
    node->prev_->next_ = node->next_;
    node->next_->prev_ = node->prev_;
}

bool LRU::Update(QueueNode *node) {
    if (!node) {
        return false;
    }

    Unlink(node);

    AddToTail(node);
    return true;
}

void LRU::Remove(QueueNode *node) {
    if (!node) {
        return;
    }

    Unlink(node);

    delete node;
}

QueueNode *LRU::Put(const int64_t key, Page *page) {
    auto *new_node = new QueueNode(key, page);
    
    AddToTail(new_node);

    return new_node;
}

Page *LRU::Evict() {
    if (IsEmpty())
        return nullptr;

    QueueNode *to_remove = dummy_->next_;
    Page *evicted_page = to_remove->page_;
    
    Remove(to_remove);
    
    return evicted_page;
}

void LRU::Clear() {
    const QueueNode *current = dummy_->next_;
    while (current != dummy_) {
        const QueueNode *temp = current;
        current = current->next_;
        delete temp;
    }
    dummy_->next_ = dummy_;
    dummy_->prev_ = dummy_;
}
