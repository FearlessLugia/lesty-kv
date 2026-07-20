//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef PAGE_H
#define PAGE_H
#include <string>
#include <utility>
#include <vector>

class Page {

public:
    std::string id_;
    std::vector<int64_t> data_;

    bool is_ephemeral_{false};
    
    explicit Page(std::string id) : id_(std::move(id)) {}
    Page(std::string id, std::vector<int64_t> data) : id_(std::move(id)), data_(std::move(data)) {}

    [[nodiscard]] size_t GetSize() const { return data_.size(); }
};


#endif // PAGE_H
