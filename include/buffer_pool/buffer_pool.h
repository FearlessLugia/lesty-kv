//
// Created by Kiiro Huang on 24-11-14.
//

#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H
#include <forward_list>
#include <string>
#include <vector>

#include "bucket_node.h"
#include "lru/lru.h"
#include "page.h"

#include "../../utils/constants.h"

class BufferPool {

public:
    std::vector<BucketNode*> *buckets_;

    size_t capacity_;
    size_t size_;

    LRU *eviction_policy_;

    explicit BufferPool(size_t capacity);
    ~BufferPool();

    [[nodiscard]] Page *Get(const std::string &id) const;

    Page *Put(const std::string &id, std::vector<int64_t> data);

    void Remove();

    void Clear();

    void Resize();

private:
    [[nodiscard]] Page *FindPage(const std::string &id) const;

    [[nodiscard]] size_t HashFunction(const std::string &key) const;
};


#endif // BUFFER_POOL_H
