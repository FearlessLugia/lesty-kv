//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef EVICTION_POLICY_H
#define EVICTION_POLICY_H
#include "../page.h"


class EvictionPolicy {
public:
    virtual void Put(int64_t key, Page *page) = 0;

    virtual void Evict() = 0;
};


#endif // EVICTION_POLICY_H
