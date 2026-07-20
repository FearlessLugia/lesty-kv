//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef EVICTION_POLICY_H
#define EVICTION_POLICY_H

#include "page.h"

class QueueNode;

class EvictionPolicy {
public:
    virtual ~EvictionPolicy() = default;

    virtual QueueNode *Put(int64_t key, Page *page) = 0;

    virtual bool Update(QueueNode *node) = 0;

    virtual Page *Evict() = 0;

    virtual void Remove(QueueNode *node) = 0;

    virtual void Clear() = 0;
};


#endif // EVICTION_POLICY_H
