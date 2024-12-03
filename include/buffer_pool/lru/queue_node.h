//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef QUEUENODE_H
#define QUEUENODE_H
#include <cstdint>

#include "../page.h"


class QueueNode {
public:
    int64_t key_;
    Page *page_;

    QueueNode* prev_;
    QueueNode* next_;

    QueueNode(const int64_t key, Page *page) : key_(key), page_(page) {}
};


#endif // QUEUENODE_H
