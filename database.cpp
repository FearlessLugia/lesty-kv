//
// Created by Kiiro Huang on 24-11-5.
//

#include "database.h"

#include <regex>
#include <sstream>
#include <unistd.h>   // for pread, close
#include <fcntl.h>

#include "constants.h"

void Database::open(const string &db_name) {
    db_name_ = db_name;

    if (!filesystem::exists(db_name)) {
        filesystem::create_directory(db_name);
        cout << "Database created: " << db_name << endl;
        sst_counter_ = 0;
    } else {
        cout << "Database opened: " << db_name << endl;

        // find the largest SST file number
        sst_counter_ = 0;
        regex filename_pattern(R"(sst_(\d+)\.bin)");
        for (const auto &entry: filesystem::directory_iterator(db_name)) {
            string filename = entry.path().filename().string();
            smatch match;
            if (regex_match(filename, match, filename_pattern)) {
                long file_counter = stol(match[1].str());
                if (file_counter >= sst_counter_) {
                    sst_counter_ = file_counter + 1; // set the next number
                }
            }
        }
    }
}

void Database::close() {
    if (memtable_.size() > 0) {
        cout << "Closing database and flushing memtable to SSTs: " << db_name_ << endl;

        flushToFile();
        memtable_.clear();
    }

    cout << "Database closed" << endl;
}

void Database::put(const int64_t &key, const int64_t &value) {
    memtable_.put(key, value);

    if (memtable_.size() >= memtable_.memtable_size) {
        cout << " Memtable is full, flushing to SSTs" << endl;
        flushToFile();
        memtable_.clear();
    }
}

optional<int64_t> Database::get(const int64_t &key) const {
    // find in memtable
    const auto value = memtable_.get(key);
    if (value.has_value()) {
        return value;
    }

    // find in SSTs from the newest to the oldest
    auto ssts = get_sorted_ssts(db_name_);

    for (const auto &entry: ssts) {
        optional<int64_t> file_value = binary_search_in_sst(entry.string(), key);
        if (file_value.has_value()) {
            return file_value;
        }
    }

    return nullopt;
}

size_t Database::getFileSize(int fd) {
    const off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size == -1) {
        cerr << "  Failed to determine file size: " << strerror(errno) << endl;
        return -1;
    }
    return static_cast<long>(file_size);
}

optional<int64_t> Database::binary_search_in_sst(const string &file_path, const int64_t &key) {
    const int fd = ::open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "  Could not open file: " << file_path << endl;
        return nullopt;
    }

    const size_t file_size = getFileSize(fd);
    const size_t num_pages = file_size / kPageSize;

    size_t left = 0;
    size_t right = num_pages - 1;

    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        off_t offset = mid * kPageSize;

        vector<char> page(kPageSize);
        ssize_t bytes_read = pread(fd, page.data(), kPageSize, offset);
        if (bytes_read < 0) {
            cout << "  Could not find key " << key << " in " << file_path << endl;
            break;
        }

        size_t num_pairs = kPageSize / kPairSize;

        const int64_t *kv_pairs = reinterpret_cast<const int64_t *>(page.data());
        const int64_t first_key = kv_pairs[0];
        const int64_t last_key = kv_pairs[(num_pairs - 1) * 2];

        if (key == first_key) {
            ::close(fd);
            cout << " Found key " << key << " in " << file_path << endl;
            return kv_pairs[1];
        }
        if (key == last_key) {
            ::close(fd);
            cout << " Found key " << key << " in " << file_path << endl;
            return kv_pairs[num_pairs * 2 - 1];
        }

        if (key > first_key && key < last_key) {
            // since the key is already in order, do another binary search to search inside the page
            size_t page_left = 0;
            size_t page_right = num_pairs - 1;

            while (page_left <= page_right) {
                const size_t page_mid = page_left + (page_right - page_left) / 2;
                const int64_t mid_key = kv_pairs[page_mid * 2];

                if (mid_key == key) {
                    ::close(fd);
                    cout << " Found key " << key << " in " << file_path << endl;
                    return kv_pairs[page_mid * 2 + 1];
                } else if (mid_key < key) {
                    page_left = page_mid + 1;
                } else {
                    page_right = page_mid - 1;
                }
            }

            ::close(fd);
            return nullopt;
        }

        if (key < first_key) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    ::close(fd);
    return nullopt;
}

vector<fs::path> Database::get_sorted_ssts(const string &path) {
    vector<filesystem::path> ssts;
    regex filename_pattern(R"(sst_(\d+)\.bin)");

    for (const auto &entry: fs::directory_iterator(path)) {
        if (entry.is_regular_file() && regex_match(entry.path().filename().string(), filename_pattern)) {
            ssts.push_back(entry.path());
        }
    }

    sort(ssts.begin(), ssts.end(), [](const fs::path &a, const fs::path &b) {
        auto time_a = fs::last_write_time(a);
        auto time_b = fs::last_write_time(b);

        return time_a > time_b;
    });

    return ssts;
}

vector<pair<int64_t, int64_t> > Database::scan(const int64_t &startKey, const int64_t &endKey) const {

}

void Database::flushToFile() {
    // Generate an increased-number-filename for the new SST file
    ostringstream filename;
    filename << db_name_ << "/sst_" << sst_counter_++ << ".bin";

    ofstream outfile(filename.str(), ios::binary);
    if (!outfile) {
        cerr << "  File could not be opened" << endl;
        exit(1);
    }

    for (const auto &[key, value]: memtable_.traverse()) {
        outfile.write(reinterpret_cast<const char *>(&key), sizeof(key));
        outfile.write(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    cout << "  Memtable flushed to SST: " << filename.str() << endl;
    outfile.close();
}
