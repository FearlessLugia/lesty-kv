//
// Created by Kiiro Huang on 24-11-20.
//

#ifndef PAGE_H
#define PAGE_H
#include <string>
#include <utility>
#include <vector>

class Page {
private:
    std::string id_;
    std::vector<int64_t> data_;
    bool is_ephemeral_{false};

public:
    explicit Page(std::string id) : id_(std::move(id)) {}
    Page(std::string id, std::vector<int64_t> data) : id_(std::move(id)), data_(std::move(data)) {}

    [[nodiscard]] size_t GetSize() const { return data_.size(); }
    [[nodiscard]] const std::string& GetId() const { return id_; }
    [[nodiscard]] const std::vector<int64_t>& GetData() const { return data_; }
    [[nodiscard]] bool IsEphemeral() const { return is_ephemeral_; }
    void SetEphemeral(bool is_ephemeral) { is_ephemeral_ = is_ephemeral; }
};


#endif // PAGE_H
