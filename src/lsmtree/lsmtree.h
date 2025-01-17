#ifndef LSMTREE_H
#define LSMTREE_H

#include <vector>
#include <string>
#include <memory>
#include <cstdint>
#include "sst/sst.h"

class LSMTree
{
public:
    explicit LSMTree(const std::string &db_name, size_t levelSizeRatio = 2);

    void addSST(const std::string &sst_filename);                      // Add a new SST file to the tree (triggered by flush)
    void addSSTToLevel(const std::string &sst_filename, size_t level); // Used during restoration
    void compact();                                                    // Perform compaction across levels
    std::vector<std::string> getSSTFilesByLevel(size_t level) const;
    size_t getNumLevels() const;
    void clearLevels();

    // helpers for testing
    void printLevels() const;
    void dumpSSTFile(const std::string &filename);

private:
    // Fixed parameters
    size_t levelSizeRatio; // Ratio between level sizes (default: 2)
    std::string db_name;

    // LSM-tree structure
    std::vector<std::vector<std::string>> levels; // SSTables organized by levels (file names)

    // Helper Functions
    void ensureLevelExists(size_t level); // Dynamically add levels as needed
    void mergeLevels(size_t level);       // Compact SSTables in a given level
    std::shared_ptr<SST> mergeTwoSSTs(const std::string &sst1, const std::string &sst2,
                                      bool isLargestLevel);
};

#endif // LSMTREE_H