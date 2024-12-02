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
    vector<int64_t> data_;

    Page(const string &id) : id_(id) {}
    Page(const string &id, const vector<int64_t> &data) : id_(id), data_(data) {}

    size_t GetSize() const { return data_.size(); }
};


#endif // PAGE_H
