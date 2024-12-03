//
// Created by Kiiro Huang on 24-11-21.
//

#ifndef BUCKET_NODE_H
#define BUCKET_NODE_H
#include "page.h"

class BucketNode {
public:
    Page *page_;

    BucketNode *next_;

    int64_t eviction_key_;

    BucketNode(Page *page) : page_(page), next_(nullptr) {}
};


#endif // BUCKET_NODE_H
