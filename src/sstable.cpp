//
// Created by Kiiro on 24-11-4.
//

#include "sstable.h"
#include "../utils/constants.h"
#include "../utils/log.h"

#include <iostream>
#include <sys/fcntl.h>
#include <unistd.h>

#include "buffer_pool/buffer_pool_manager.h"
#include "buffer_pool/page.h"


using namespace std;

SSTable::SSTable(const filesystem::path &file_path) : file_path_(file_path) {
    fd_ = open(file_path.c_str(), O_RDONLY);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open SSTable file.");
    }

    file_size_ = GetFileSize();
    InitialKeyRange();
}

SSTable::~SSTable() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

off_t SSTable::GetFileSize() const {
    const off_t file_size = lseek(fd_, 0, SEEK_END);
    if (file_size == -1) {
        cerr << "  Failed to determine file size: " << strerror(errno) << endl;
        return -1;
    }
    return file_size;
}

// update min key and max key of the SSTable
void SSTable::InitialKeyRange() {
    char buffer[kPageSize];

    // Read the first block to get the minimum key
    ssize_t bytes_read = pread(fd_, buffer, kPageSize, 0);
    if (bytes_read > 0) {
        size_t pos = 0;
        pair<int64_t, int64_t> first_entry;
        if (ReadEntry(buffer, bytes_read, pos, first_entry)) {
            min_key_ = first_entry.first;
        }
    }

    // Read the last block to get the maximum key
    const off_t last_block_offset = file_size_ > kPageSize ? file_size_ - kPageSize : 0;
    bytes_read = pread(fd_, buffer, kPageSize, last_block_offset);
    if (bytes_read > 0) {
        size_t pos = 0;
        pair<int64_t, int64_t> last_entry;
        while (pos < static_cast<size_t>(bytes_read) && ReadEntry(buffer, bytes_read, pos, last_entry)) {
            // Keep reading to get the last entry in the block
        }
        max_key_ = last_entry.first;
    }
}

bool SSTable::ReadEntry(const char *buffer, const size_t buffer_size, size_t &pos,
                        pair<int64_t, int64_t> &entry) const {
    // Ensure there is enough space in the buffer for both key and value (2 x int64_t)
    if (pos + 2 * sizeof(int64_t) > buffer_size) {
        return false; // Not enough data left in the buffer
    }

    // Read key as int64_t
    memcpy(&entry.first, buffer + pos, sizeof(int64_t));
    pos += sizeof(int64_t);

    // Read value as int64_t
    memcpy(&entry.second, buffer + pos, sizeof(int64_t));
    pos += sizeof(int64_t);

    return true;
}

Page *SSTable::GetPage(off_t offset, bool is_sequential_flooding = false) const {
    // Concatenate the name of the file with the offset to get the page id
    const size_t start_pos = file_path_.find('/') + 1;
    const size_t end_pos = file_path_.rfind(".bin");
    const string sst_name = file_path_.substr(start_pos, end_pos - start_pos);

    string page_id = sst_name + "_" + to_string(offset);

    auto buffer_pool = BufferPoolManager::GetInstance();
    Page *exist_page = buffer_pool->Get(page_id);
    if (exist_page != nullptr) {
        return exist_page;
    }

    // If the page is not in the buffer pool, read it from disk
    char buffer[kPageSize];
    // Align the offset to the beginning of the page
    const off_t aligned_offset = offset & ~static_cast<off_t>(kPagePairs - 1);
    const ssize_t bytes_read = pread(fd_, buffer, kPageSize, aligned_offset);
    if (bytes_read <= 0) {
        LOG("\tCould not read page at offset " << offset << " in " << file_path_);
        return nullptr;
    }

    vector<int64_t> data;
    size_t pos = 0;
    pair<int64_t, int64_t> entry;
    while (ReadEntry(buffer, static_cast<size_t>(bytes_read), pos, entry)) {
        data.push_back(entry.first);
        data.push_back(entry.second);
    }

    // if not sequential flooding, put the page into the buffer pool
    if (!is_sequential_flooding) {
        auto buffer_pool = BufferPoolManager::GetInstance();
        buffer_pool->Put(page_id, data);
    }

    Page *new_page = new Page(page_id, data);
    return new_page;
}

optional<int64_t> SSTable::Get(const int64_t key) const {
    // if max key is smaller than key, no need to scan
    // if min key is larger than key, no need to scan
    if (max_key_ < key || min_key_ > key) {
        LOG("\t\tno value in this sst");
        return nullopt;
    }

    return BinarySearch(key);
}

optional<int64_t> SSTable::BinarySearch(const int64_t key) const {
    const size_t num_pages = (file_size_ + kPageSize - 1) / kPageSize;

    size_t left = 0;
    size_t right = num_pages - 1;

    while (left <= right) {
        const size_t mid = left + (right - left) / 2;
        const off_t offset = mid * kPageSize;

        const Page *page = GetPage(offset);
        const auto data = page->data_;
        const size_t num_pairs = page->GetSize() / 2;

        const int64_t first_key = data[0];
        const int64_t last_key = data[(num_pairs - 1) * 2];

        if (key == first_key) {
            ::close(fd_);
            LOG("\t\tFound key " << key << " in " << file_path_);
            return data[1];
        }
        if (key == last_key) {
            ::close(fd_);
            LOG("\t\tFound key " << key << " in " << file_path_);
            return data[num_pairs * 2 - 1];
        }

        if (key > first_key && key < last_key) {
            // since the key is already in order, do another binary search to search inside the page
            size_t page_left = 0;
            size_t page_right = num_pairs - 1;

            while (page_left <= page_right) {
                const size_t page_mid = page_left + (page_right - page_left) / 2;
                const int64_t mid_key = data[page_mid * 2];

                if (mid_key == key) {
                    ::close(fd_);
                    LOG("\t\tFound key " << key << " in " << file_path_);
                    return data[page_mid * 2 + 1];
                }

                if (mid_key < key) {
                    page_left = page_mid + 1;
                } else {
                    page_right = page_mid - 1;
                }
            }

            ::close(fd_);
            return nullopt;
        }

        if (key < first_key) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    LOG("  2 Could not find key " << key << " in " << file_path_);
    ::close(fd_);
    return nullopt;
}

vector<pair<int64_t, int64_t>> SSTable::Scan(const int64_t start_key, const int64_t end_key) const {
    vector<pair<int64_t, int64_t>> result;

    // max key is smaller than start key, no need to scan
    // min key is larger than end key, no need to scan
    if (max_key_ < start_key || min_key_ > end_key) {
        LOG("\t\tno value in this sst");

        return result;
    }

    int64_t start_offset;
    if (min_key_ > start_key) {
        // min key is larger than end key, scan from the beginning
        LOG("\t\tMin key " << min_key_ << " is larger than start key " << start_key << ", scan from the beginning");

        start_offset = 0;
    } else {
        const bool is_sequential_flooding = (end_key - start_key + 1) / kPagePairs >= kPageSequentialFlooding;
        if (is_sequential_flooding) {
            LOG("  Scan range exceeds sequential flooding threshold, skipping buffer pool writes");
        };

        // else, binary search to find the upper bound of the start key
        start_offset = BinarySearchUpperbound(start_key, is_sequential_flooding);
    }
    LOG("\t\t\tStart offset: " << start_offset);


    // Found start key, linear search to find end key
    const auto values = LinearSearchToEndKey(start_offset, start_key, end_key);
    for (const auto &[key, value]: values) {
        result.emplace_back(key, value);
    }
    LOG("\t\tresult size: " << result.size());

    return result;
}

int64_t SSTable::BinarySearchUpperbound(const int64_t key, bool is_sequential_flooding = false) const {
    const size_t num_pages = (file_size_ + kPageSize - 1) / kPageSize;

    size_t left = 0;
    size_t right = num_pages;

    // Outer binary search to find the first page where first_key > key
    while (left < right) {
        const size_t mid = left + (right - left) / 2;
        const off_t offset = mid * kPageSize;

        const Page *page = GetPage(offset);
        const auto data = page->data_;
        const size_t num_pairs = page->GetSize() / 2;

        const int64_t first_key = data[0];

        if (first_key <= key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if (left - 1 == num_pages) {
        // The key is greater than or equal to all keys in the SSTable
        return -1;
    }

    // Read the page where the upper bound could be
    const size_t page_index = left - 1;
    const off_t page_offset = page_index * kPageSize;

    const Page *page = GetPage(page_offset);
    const auto data = page->data_;
    const size_t num_pairs = page->GetSize() / 2;

    const int64_t first_key = data[0];
    const int64_t last_key = data[(num_pairs - 1) * 2];

    // Inner binary search to find the upper bound within the page
    size_t page_left = 0;
    size_t page_right = num_pairs;

    while (page_left < page_right) {
        const size_t page_mid = page_left + (page_right - page_left) / 2;
        const int64_t mid_key = data[page_mid * 2];

        if (mid_key <= key) {
            page_left = page_mid + 1;
        } else {
            page_right = page_mid;
        }
    }

    if (page_left - 1 == num_pairs) {
        // All keys in this page are less than or equal to the given key
        // Check if there is a next page
        if (page_index + 1 <= num_pages) {
            if (page_index + 1 < num_pages) {
                const off_t next_page_offset = (page_index + 1) * kPageSize;
                return next_page_offset; // Upper bound starts at the next page
            }
            // No more keys available
            return -1;
        }
        // No next page; the given key is greater than all keys
        return -1;
    }

    // Upper bound found within the page
    const off_t key_offset = page_offset + (page_left - 1) * kPairSize;
    return key_offset;
}

vector<pair<int64_t, int64_t>> SSTable::LinearSearchToEndKey(off_t start_offset, int64_t start_key,
                                                             int64_t end_key) const {
    vector<pair<int64_t, int64_t>> result;

    auto current_offset = start_offset;

    while (true) {
        const Page *page = GetPage(current_offset);

        // When start key is the last key in the SSTable, next page will be nullptr
        if (page == nullptr) {
            return result;
        }

        const auto data = page->data_;
        const size_t num_pairs = page->GetSize() / 2;

        for (size_t i = 0; i < num_pairs; i++) {
            if (data[i * 2] > end_key) {
                return result;
            }

            if (data[i * 2] >= start_key) {
                result.emplace_back(data[i * 2], data[i * 2 + 1]);
            }
        }

        current_offset += kPageSize;
    }
}
