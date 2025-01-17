
#ifndef KVSTORE_H
#define KVSTORE_H

#include <string>
#include <deque>
#include <vector>
#include "memtable/memtable.h"
#include "lsmtree/lsmtree.h"

class KVStore
{
private:
    AVLTree memtable; // In-memory AVL Tree for fast access
    std::unique_ptr<LSMTree> lsmTree;
    std::string db_name; // Database name (used for file storage path)
    int memtable_size;   // Size threshold for the memtable
    int sst_counter;     // Counter for SST files

    // Helper function to flush memtable to SST
    void flushMemtableToSST();

    // Helper function to read SST files and perform binary search
    int64_t binarySearchSST(int sst_fd, const std::string &sst_filename, int64_t target_key);

    // Helper function to read SST files and perform btree search
    int64_t btreeSearchSST(int sst_fd, int64_t target_key, const std::string &sst_filename);
    int64_t searchInPage(const char *pageBuffer, int64_t target_key);
    int64_t searchInNode(char *nodeBuffer, int64_t target_key, int sst_fd, const std::string &sst_filename, off_t pageStartOffset, off_t pageEndOffset);
    int64_t followOffset(int sst_fd, int64_t offset, int64_t target_key, const std::string &sst_filename, off_t pageStartOffset, off_t pageEndOffset);

    // Helper function to scan SST files and return key-value pairs in a range
    std::vector<std::pair<int64_t, int64_t>> scanSST(int sst_fd, const std::string &sst_filename, int64_t start, int64_t end);

    std::vector<std::pair<int64_t, int64_t>> scanBtree(int sst_fd, const std::string &sst_filename, int64_t start, int64_t end);
    void scanNode(int sst_fd, const std::string &sst_filename, off_t offset, int64_t start, int64_t end, off_t pageStartOffset, off_t pageEndOffset, std::vector<std::pair<int64_t, int64_t>> &result);
    void scanPage(const char *pageBuffer, int64_t start, int64_t end, std::vector<std::pair<int64_t, int64_t>> &result);

    std::vector<std::pair<int64_t, int64_t>> mergedScan(int64_t start, int64_t end);

    // **Added flag to indicate the use of B-tree search**
    bool useBTree = false;

public:
    KVStore(int memtable_size, size_t levelSizeRatio = 2);

    // Open the database
    void Open(const std::string &database_name);

    // Close the database
    void Close();

    // Insert a key-value pair
    void Put(int64_t key, int64_t value);

    // Delete a key-value pair
    void Del(int64_t key);

    // Retrieve a value by key
    int64_t Get(int64_t key);

    // Scan for key-value pairs in a range [start, end]
    std::pair<int64_t, int64_t> *Scan(int64_t start, int64_t end, int &result_count);

    // **Method to set the search method (B-tree or binary search)**
    void SetUseBTree(bool flag);
};

#endif