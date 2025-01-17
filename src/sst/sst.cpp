#include "sst.h"
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include "btree/btree.h" // Include the BTree header
#include "global/globals.h"
#include "bloomfilter.h"

SST::SST() : startingKey(0), endingKey(0), numEntries(0), numPages(0), bloomFilter(NUM_ENTRIES, BITS_PER_ENTRY) {}

//////////////////////////////////////////
// YOU CAN MODIFY THE CONTENTS OF THIS FILE
//////////////////////////////////////////
void SST::addPage(const Page &page)
{
    // Update SST metadata
    if (numPages == 0)
    {
        startingKey = page.startingKey; // Set the starting key from the first page
    }
    endingKey = page.keys.back().key; // Set the ending key from the last entry in this page
    numEntries += page.numEntries;
    numPages++;

    // Add the page to the collection
    pages.push_back(page);

    // Add all keys in the page to the Bloom filter
    for (const auto &entry : page.keys)
    {
        bloomFilter.insert(entry.key); // Add keys to Bloom filter
    }
    bloomFilter.updateData();
    // Calculate the offset of this page in the SST file
    // Assuming that pages are written sequentially after the SST metadata
    int64_t pageOffset = SST_METADATA_SIZE + PAGE_SIZE + (numPages - 1) * PAGE_SIZE;
    pageOffsets.push_back(pageOffset);

    // Build the B-tree incrementally
    if (!btree)
    {
        // std::cout << "Initialize new btree" << std::endl;
        btree = new BTree(BTREE_DEGREE); // Initialize B-tree with specified degree
    }

    // Insert the ending key of the page and its offset into the B-tree
    btree->insert(endingKey, pageOffset);

    // std::cout << "A new B-tree node with key " << endingKey << " is inserted" << std::endl;
    // std::cout << "The is what the btree looks like right now:" << std::endl;
    // btree->printTree();
    // std::cout << "End. Please see the tree in the next round" << std::endl;
}

// write to file
void SST::writeToFile(const std::string &filename)
{
    int sst_fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (sst_fd == -1)
    {
        throw std::runtime_error("Failed to open SST file for writing.");
    }

    off_t offset = 0; // Start writing at the beginning of the file
    size_t totalBytesWritten = 0;

    // Step 1: Write SST-level metadata
    pwrite(sst_fd, &numEntries, sizeof(numEntries), offset);
    offset += sizeof(numEntries);
    totalBytesWritten += sizeof(numEntries);

    pwrite(sst_fd, &numPages, sizeof(numPages), offset);
    offset += sizeof(numPages);
    totalBytesWritten += sizeof(numPages);

    pwrite(sst_fd, &startingKey, sizeof(startingKey), offset);
    offset += sizeof(startingKey);
    totalBytesWritten += sizeof(startingKey);

    pwrite(sst_fd, &endingKey, sizeof(endingKey), offset);
    offset += sizeof(endingKey);
    totalBytesWritten += sizeof(endingKey);

    bloomFilter.updateData();
    // bloomFilter.printData();
    std::cout << "Bytes written before write bloom: " << totalBytesWritten << std::endl;
    // Write Bloom filter metadata and bit array
    ssize_t bloomFilterSize = bloomFilter.data.size();

    // Write size of the bit array
    pwrite(sst_fd, bloomFilter.data.data(), bloomFilterSize, offset);
    offset += bloomFilterSize;
    totalBytesWritten += bloomFilterSize;

    std::cout << "Bytes written after write bloom: " << totalBytesWritten << std::endl;

    // Step 2: Write each page (entire page data is stored in page.data)
    for (const auto &page : pages)
    {
        // Write the entire page data (consolidated as `page.data`)
        ssize_t pageDataSize = page.data.size();
        pwrite(sst_fd, page.data.data(), pageDataSize, offset);
        offset += pageDataSize;
        totalBytesWritten += pageDataSize;
    }

    // Get the vector of nodes in pre-order traversal
    std::vector<BTree::Node *> preOrderNodes = btree->preorderTraversal();

    postorderTraversalWrite(btree->getRoot(), offset, totalBytesWritten, sst_fd);

    close(sst_fd);
}

void SST::postorderTraversalWrite(BTree::Node *node, off_t &currentOffset, size_t &BytesWritten, int &fd)
{
    if (!node)
        return;

    // Recursively visit each child first
    if (!node->isLeaf)
    {
        for (auto child : node->children)
        {
            node->offsets.push_back(currentOffset);
            postorderTraversalWrite(child, currentOffset, BytesWritten, fd);
        }
    }
    node->updateData();
    ssize_t nodeDataSize = node->data.size();
    pwrite(fd, node->data.data(), nodeDataSize, currentOffset);
    currentOffset += nodeDataSize;
    BytesWritten += nodeDataSize;
}

bool SST::mightContain(int64_t key) const
{
    return bloomFilter.query(key);
}