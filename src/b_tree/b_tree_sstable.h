//
// Created by Kiiro Huang on 24-11-21.
//

#ifndef B_TREE_H
#define B_TREE_H
#include <sys/_types/_off_t.h>
#include "../buffer_pool/page.h"
#include "../memtable.h"
#include "../sstable.h"

class BTreeSSTable : public SSTable {
public:
    BTreeSSTable(const filesystem::path &path);

    bool WriteEntry(char *buffer, const size_t buffer_size, size_t &pos, pair<int64_t, int64_t> &entry);

    void WritePage(const off_t offset, const Page *page) const;

    void FlushFromMemtable(const vector<int64_t> *data);
};


#endif // B_TREE_H
