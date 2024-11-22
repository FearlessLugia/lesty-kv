//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef PAGE_H
#define PAGE_H
#include <string>
#include <vector>

using namespace std;

class Page {
public:
    string id_;
    const vector<int64_t> *data_;

    // Page *hash_next;

    Page(const string &id) : id_(id), data_{} {}

    const int64_t *getData() const { return data_->data(); }

    // int64_t eviction_policy_key_;

    // void setData(const int64_t *src, size_t size) {
    //     memcpy(data_->data(), src, size < data_->size() ? size : data_->size());
    // }
};


#endif // PAGE_H
