//
// Created by Kiiro Huang on 24-11-21.
//

#ifndef BUCKET_NODE_H
#define BUCKET_NODE_H
#include "page.h"

class QueueNode;

class BucketNode {
public:
    Page *page_;

    BucketNode *next_;

    int64_t eviction_key_{0};

    QueueNode *lru_node_{nullptr};

    explicit BucketNode(Page *page) : page_(page), next_(nullptr) {}
    BucketNode(Page *page, const int64_t eviction_key) : page_(page), next_(nullptr), eviction_key_(eviction_key) {}
};


#endif // BUCKET_NODE_H
