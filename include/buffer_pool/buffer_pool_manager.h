//
// Created by Kiiro Huang on 2024-11-22.
//

#ifndef BUFFER_POOL_MANAGER_H
#define BUFFER_POOL_MANAGER_H
#include "buffer_pool.h"

// a singleton class to manage the buffer pool
class BufferPoolManager {
public:
    static BufferPool *GetInstance(const size_t pool_size = kBufferPoolSize) {
        static BufferPool instance(pool_size);
        return &instance;
    }

private:
    BufferPoolManager() = default;
    ~BufferPoolManager() = default;

    BufferPoolManager(const BufferPoolManager &) = delete;
    BufferPoolManager &operator=(const BufferPoolManager &) = delete;
};


#endif // BUFFER_POOL_MANAGER_H
