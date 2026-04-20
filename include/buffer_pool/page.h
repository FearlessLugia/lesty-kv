//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef PAGE_H
#define PAGE_H
#include <string>
#include <vector>

class Page {

public:
    std::string id_;
    std::vector<int64_t> data_;

    int eviction_policy_key_;

    Page(const std::string &id) : id_(id) {}
    Page(const std::string &id, const std::vector<int64_t> &data) : id_(id), data_(data) {}

    size_t GetSize() const { return data_.size(); }
};


#endif // PAGE_H
