//
// Created by Kiiro Huang on 24-11-20.
//

#include "lru.h"

LRU::LRU(const size_t capacity) : capacity_(capacity) {}

LRU::~LRU() {
    while (front_) {
        QueueNode *temp = front_;
        front_ = front_->next_;
        delete temp;
    }
}

void LRU::MoveToTail(QueueNode *node) {
    if (node == rear_)
        return;

    if (node == front_) {
        front_ = front_->next_;
        front_->prev_ = nullptr;
    } else {
        node->prev_->next_ = node->next_;
        node->next_->prev_ = node->prev_;
    }

    node->next_ = nullptr;
    node->prev_ = rear_;
    rear_->next_ = node;
    rear_ = node;
}

bool LRU::Update(Page *page) {
    QueueNode *current = front_;
    while (current) {
        // find the page in the queue
        if (current->page_ == page) {
            // move the page to the rear of the queue
            MoveToTail(current);
            return true;
        }
        current = current->next_;
    }
    return false;
}

void LRU::Put(const int64_t key, Page *page) {
    // if the page is already in the LRU queue
    if (Update(page)) {
        return;
    }

    // create a new node
    QueueNode *new_node = new QueueNode(key, page);
    if (!front_) {
        front_ = rear_ = new_node;
    } else {
        rear_->next_ = new_node;
        new_node->prev_ = rear_;
        rear_ = new_node;
    }
    ++size_;

    // when the queue reaches the capacity, evict the least recently used page
    if (size_ > capacity_) {
        Evict();
    }

    // // if the page is not in the LRU queue
    // if (page->eviction_policy_key_ != key) {
    //     // when the queue reaches the capacity, evict the least recently used page
    //     if (queue_->size() == capacity_) {
    //         Evict();
    //     }
    //
    //     // insert the new page to the front of the queue
    //
    //     queue_->push_front(QueueNode(key, page));
    //     // update the eviction policy key of the page
    //     page->eviction_policy_key_ = key;
    // } else {
    //     // if the page is already in the queue
    //     // find the page in the queue
    //     const auto it =
    //             std::find_if(queue_->begin(), queue_->end(), [&](const QueueNode &node) { return node.page_ == page;
    //             });
    //
    //     // move the page to the rear of the queue
    //     queue_->splice(queue_->end(), *queue_, it);
    // }
}

void LRU::Evict() {
    if (!front_)
        return;

    QueueNode *to_remove = front_;
    if (front_ == rear_) {
        front_ = rear_ = nullptr;
    } else {
        front_ = front_->next_;
        front_->prev_ = nullptr;
    }

    delete to_remove;

    --size_;
}

void LRU::Clear() {
    while (front_) {
        QueueNode *temp = front_;
        front_ = front_->next_;
        delete temp;
    }
    rear_ = nullptr;
    size_ = 0;
}
