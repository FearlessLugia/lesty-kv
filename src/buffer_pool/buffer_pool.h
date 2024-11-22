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

using namespace std;



class BufferPool {

public:
    vector<BucketNode*> *buckets_;
    // vector<forward_list<Page*>> *buckets_;

    size_t capacity_;
    size_t size_;

    LRU *eviction_policy_;

    explicit BufferPool(size_t capacity);
    ~BufferPool();


    Page *Get(const string &id) const;

    Page *Put(const string &id, const vector<int64_t> &data);


    void Remove();

    void Clear();

    void Resize();

private:
    Page *FindPage(const string &id) const;

    size_t HashFunction(const string &key) const;
};


#endif // BUFFER_POOL_H
