#ifndef SST_H
#define SST_H

#include <string>
#include <vector>
#include <cstdint>
#include "page.h"
#include "btree/btree.h" // Include the BTree header
#include "global/globals.h"
#include "bloomfilter.h"

class SST
{
public:
    SST(); // Constructor

    // Adds a page to the SST
    void addPage(const Page &page);

    // Flushes the SST to disk, writing all pages and metadata
    void writeToFile(const std::string &filename);

    void postorderTraversalWrite(BTree::Node* node, off_t& currentOffset, size_t& BytesWritten, int& fd);

    bool mightContain(int64_t key) const;          // Query Bloom filter
    
    // Metadata fields
    int64_t startingKey; // Key range for the SST
    int64_t endingKey;
    int numEntries = 0; // Total number of entries in the SST
    int numPages = 0;   // Total number of pages in the SST

private:
    std::vector<Page> pages; // Collection of pages in this SST

    // Offsets of pages within the SST file
    std::vector<int64_t> pageOffsets;

    // B-tree built over the pages
    BTree* btree = nullptr;

    BloomFilter bloomFilter; // Bloom filter for quick key lookups

};

#endif