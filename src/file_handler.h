//
// Created by Kiiro Huang on 2024-12-01.
//

#ifndef FILEHANDLER_H
#define FILEHANDLER_H

#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>

class FileHandler {
    int fd_;
    std::string file_path_;

public:
    // 构造函数：打开文件
    explicit FileHandler(const std::string &file_path, int flags = O_RDONLY) : fd_(-1), file_path_(file_path) {
        fd_ = open(file_path.c_str(), flags);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open file: " + file_path);
        }
    }

    // 禁用拷贝操作，防止多个实例操作同一个文件描述符
    FileHandler(const FileHandler &) = delete;
    FileHandler &operator=(const FileHandler &) = delete;

    // 移动操作
    FileHandler(FileHandler &&other) noexcept : fd_(other.fd_), file_path_(std::move(other.file_path_)) {
        other.fd_ = -1; // 避免原对象析构时关闭文件
    }

    FileHandler &operator=(FileHandler &&other) noexcept {
        if (this != &other) {
            Close();
            fd_ = other.fd_;
            file_path_ = std::move(other.file_path_);
            other.fd_ = -1;
        }
        return *this;
    }

    // 获取文件描述符
    int GetFd() const {
        if (fd_ < 0) {
            throw std::runtime_error("Invalid file descriptor for file: " + file_path_);
        }
        return fd_;
    }

    // 关闭文件
    void Close() {
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
    }

    // 析构函数：确保文件被关闭
    ~FileHandler() {
        try {
            Close();
        } catch (const std::exception &e) {
            std::cerr << "Error closing file: " << e.what() << std::endl;
        }
    }
};


#endif //FILEHANDLER_H
