#include <iostream>
#include <stdexcept>
#include <unordered_map>
#include <algorithm> // For std::sort and std::unique
#include "lsmtree.h"
#include "page/page.h"
#include "global/globals.h"
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>

LSMTree::LSMTree(const std::string &db_name, size_t levelSizeRatio)
    : db_name(db_name), levelSizeRatio(levelSizeRatio)
{
    ensureLevelExists(0); // Start with the first level
}

void LSMTree::clearLevels()
{
    for (auto &level : levels)
    {
        level.clear();
    }
    levels.clear();
}

void LSMTree::dumpSSTFile(const std::string &sst_filename)
{
    int sst_fd = open(sst_filename.c_str(), O_RDONLY);
    if (sst_fd == -1)
    {
        std::cerr << "ERROR: Failed to open SST file for reading: " << sst_filename << std::endl;
        return;
    }

    // Determine the size of the file
    off_t file_size = lseek(sst_fd, 0, SEEK_END);
    if (file_size == -1)
    {
        std::cerr << "ERROR: Failed to determine file size for: " << sst_filename << std::endl;
        close(sst_fd);
        return;
    }

    // Reset file pointer to the beginning
    lseek(sst_fd, 0, SEEK_SET);

    // Allocate a buffer for the entire file
    std::vector<char> sst_file(file_size);

    // Read the entire SST file into memory
    ssize_t bytes_read = read(sst_fd, sst_file.data(), file_size);
    if (bytes_read == -1 || bytes_read != file_size)
    {
        std::cerr << "ERROR: Failed to read the entire SST file for: " << sst_filename << std::endl;
        close(sst_fd);
        return;
    }

    close(sst_fd);

    // Dump the SST file contents in hex
    std::cout << "SST File Dump (" << sst_filename << ", " << file_size << " bytes):\n";
    for (size_t i = 0; i < file_size; ++i)
    {
        printf("%02x ", static_cast<unsigned char>(sst_file[i]));
        if ((i + 1) % 16 == 0)
            printf("\n");
    }
    printf("\n");
}

void LSMTree::ensureLevelExists(size_t level)
{
    while (levels.size() <= level)
    {
        levels.emplace_back(); // Add an empty vector of SSTs for the new level
    }
}

size_t LSMTree::getNumLevels() const
{
    return levels.size();
}

std::vector<std::string> LSMTree::getSSTFilesByLevel(size_t level) const
{
    if (level < levels.size())
    {
        return levels[level];
    }
    return {};
}

void LSMTree::printLevels() const
{
    std::cout << "DEBUG: Current state of LSM Tree levels:" << std::endl;

    for (size_t i = 0; i < levels.size(); ++i)
    {
        std::cout << "  Level " << i << ": ";

        if (levels[i].empty())
        {
            std::cout << "(empty)";
        }
        else
        {
            for (const auto &filename : levels[i])
            {
                std::cout << filename << " ";
            }
        }

        std::cout << std::endl;
    }
}

void LSMTree::addSSTToLevel(const std::string &sst_filename, size_t level)
{
    ensureLevelExists(level);
    levels[level].emplace_back(sst_filename);
    std::cout << "Added SST file " << sst_filename << " to level " << level << std::endl;
}

void LSMTree::addSST(const std::string &sstFileName)
{
    // Log the SST being added
    std::cout << "DEBUG: Adding SST file: " << sstFileName << " to Level 0." << std::endl;

    // Ensure Level 0 exists in the levels structure
    ensureLevelExists(0);

    // Add the new SST filename to Level 0
    levels[0].push_back(sstFileName);

    // Log the current size of Level 0
    std::cout << "DEBUG: Level 0 size after addition: " << levels[0].size() << std::endl;

    // Compact as soon as the level reaches the size ratio
    if (levels[0].size() == levelSizeRatio)
    {
        std::cout << "DEBUG: Level 0 reached size ratio (" << levelSizeRatio << "). Triggering compaction." << std::endl;
        compact(); // Trigger compaction if needed
    }

    printLevels();
}

void LSMTree::compact()
{
    mergeLevels(0); // Start compaction from Level 0
}

void LSMTree::mergeLevels(size_t level)
{
    // Ensure the next level exists before merging
    ensureLevelExists(level + 1);

    // Check if there are exactly two SSTs to merge at the current level
    if (levels[level].size() != levelSizeRatio)
    {
        return; // No merge needed
    }

    // Extract the filenames of the two SSTs to merge
    std::string sst1_filename = levels[level][0];
    std::string sst2_filename = levels[level][1];

    bool isLargestLevel = (level + 1) == (getNumLevels() - 1);
    // Perform the merge
    std::shared_ptr<SST> mergedSST = mergeTwoSSTs(sst1_filename, sst2_filename, isLargestLevel);

    auto extractNumericSuffix = [](const std::string &filename) -> int
    {
        size_t start = filename.find_last_of('_') + 1;
        size_t end = filename.find_last_of('.');
        return std::stoi(filename.substr(start, end - start));
    };

    int suffix1 = extractNumericSuffix(sst1_filename);
    int suffix2 = extractNumericSuffix(sst2_filename);

    // Combine the numeric suffixes in ascending order
    std::string merged_suffix = (suffix1 < suffix2)
                                    ? std::to_string(suffix1) + "_" + std::to_string(suffix2)
                                    : std::to_string(suffix2) + "_" + std::to_string(suffix1);

    // Construct the merged filename
    std::string merged_filename = db_name + "/sst_" + merged_suffix + ".sst";

    // Write the merged SST to the file
    mergedSST->writeToFile(merged_filename);

    // Remove the old SSTs from the current level (both in memory and on disk)
    if (std::remove(sst1_filename.c_str()) != 0)
    {
        std::cerr << "Warning: Failed to delete file " << sst1_filename << std::endl;
    }
    if (std::remove(sst2_filename.c_str()) != 0)
    {
        std::cerr << "Warning: Failed to delete file " << sst2_filename << std::endl;
    }

    // Remove the old SSTs from the current level
    levels[level].clear();

    // Add the merged SST to the next level
    levels[level + 1].push_back(merged_filename);

    // If the next level exceeds the size ratio, recursively compact it
    if (levels[level + 1].size() == levelSizeRatio)
    {
        mergeLevels(level + 1);
    }
}

std::shared_ptr<SST> LSMTree::mergeTwoSSTs(const std::string &sst1_filename, const std::string &sst2_filename, bool isLargestLevel)
{
    // Open SST files
    int sst1_fd = open(sst1_filename.c_str(), O_RDONLY);
    int sst2_fd = open(sst2_filename.c_str(), O_RDONLY);

    if (sst1_fd == -1 || sst2_fd == -1)
    {
        throw std::runtime_error("Failed to open SST files for merging.");
    }

    // Read numPages metadata
    int numpages1, numpages2;
    ssize_t bytesRead1 = pread(sst1_fd, &numpages1, sizeof(int), sizeof(int)); // Offset 4 (after numEntries)
    ssize_t bytesRead2 = pread(sst2_fd, &numpages2, sizeof(int), sizeof(int));

    if (bytesRead1 != sizeof(int) || bytesRead2 != sizeof(int))
    {
        close(sst1_fd);
        close(sst2_fd);
        throw std::runtime_error("Failed to read numPages from SST files.");
    }

    // Initialize offsets to the start of the first page
    off_t offset1 = SST_METADATA_SIZE + PAGE_SIZE;
    off_t offset2 = SST_METADATA_SIZE + PAGE_SIZE;

    // Initialize buffers for pages
    char buffer1[PAGE_SIZE] = {0};
    char buffer2[PAGE_SIZE] = {0};

    int currentPage1 = 0, currentPage2 = 0;

    // Read the first pages into buffers
    bytesRead1 = pread(sst1_fd, buffer1, PAGE_SIZE, offset1);
    bytesRead2 = pread(sst2_fd, buffer2, PAGE_SIZE, offset2);

    if (bytesRead1 != PAGE_SIZE || bytesRead2 != PAGE_SIZE)
    {
        close(sst1_fd);
        close(sst2_fd);
        throw std::runtime_error("Failed to read initial pages from SST files.");
    }

    // Extract number of entries from each page
    int numEntries1, numEntries2;
    std::memcpy(&numEntries1, buffer1, sizeof(int)); // Extract numEntries for SST1
    std::memcpy(&numEntries2, buffer2, sizeof(int)); // Extract numEntries for SST2

    // Initialize positions to read key-offset pairs after page metadata
    size_t keyOffsetPos1 = sizeof(int) + sizeof(int64_t) + sizeof(int); // Adjust based on actual metadata layout
    size_t keyOffsetPos2 = sizeof(int) + sizeof(int64_t) + sizeof(int);

    // Extract Key-Offset pairs for SST1
    std::vector<KeyOffset> keys1;
    for (int i = 0; i < numEntries1; ++i)
    {
        if (keyOffsetPos1 + sizeof(int64_t) + sizeof(int) > PAGE_SIZE)
        {
            close(sst1_fd);
            close(sst2_fd);
            throw std::runtime_error("KeyOffsetPos1 exceeds PAGE_SIZE.");
        }

        KeyOffset keyOffset;
        std::memcpy(&keyOffset.key, buffer1 + keyOffsetPos1, sizeof(int64_t));
        std::memcpy(&keyOffset.valueOffset, buffer1 + keyOffsetPos1 + sizeof(int64_t), sizeof(int));
        keys1.push_back(keyOffset);
        keyOffsetPos1 += sizeof(int64_t) + sizeof(int);
    }

    // Extract Key-Offset pairs for SST2
    std::vector<KeyOffset> keys2;
    for (int i = 0; i < numEntries2; ++i)
    {
        if (keyOffsetPos2 + sizeof(int64_t) + sizeof(int) > PAGE_SIZE)
        {
            close(sst1_fd);
            close(sst2_fd);
            throw std::runtime_error("KeyOffsetPos2 exceeds PAGE_SIZE.");
        }

        KeyOffset keyOffset;
        std::memcpy(&keyOffset.key, buffer2 + keyOffsetPos2, sizeof(int64_t));
        std::memcpy(&keyOffset.valueOffset, buffer2 + keyOffsetPos2 + sizeof(int64_t), sizeof(int));
        keys2.push_back(keyOffset);
        keyOffsetPos2 += sizeof(int64_t) + sizeof(int);
    }

    // Prepare the output SST
    auto mergedSST = std::make_shared<SST>();
    Page outputPage;

    size_t keyIndex1 = 0, keyIndex2 = 0;

    // Begin merging
    while (currentPage1 < numpages1 || currentPage2 < numpages2)
    {
        // Reload page from SST1 if keys1 are exhausted and more pages are available
        if (keyIndex1 >= keys1.size() && (currentPage1 + 1) < numpages1)
        {
            offset1 += PAGE_SIZE;
            bytesRead1 = pread(sst1_fd, buffer1, PAGE_SIZE, offset1);

            if (bytesRead1 != PAGE_SIZE)
            {
                close(sst1_fd);
                close(sst2_fd);
                throw std::runtime_error("Failed to read a page from SST1.");
            }

            std::memcpy(&numEntries1, buffer1, sizeof(int)); // Update numEntries for new page
            keys1.clear();

            keyOffsetPos1 = sizeof(int) + sizeof(int64_t) + sizeof(int);
            for (int i = 0; i < numEntries1; ++i)
            {
                if (keyOffsetPos1 + sizeof(int64_t) + sizeof(int) > PAGE_SIZE)
                {
                    close(sst1_fd);
                    close(sst2_fd);
                    throw std::runtime_error("KeyOffsetPos1 exceeds PAGE_SIZE during page reload.");
                }

                KeyOffset keyOffset;
                std::memcpy(&keyOffset.key, buffer1 + keyOffsetPos1, sizeof(int64_t));
                std::memcpy(&keyOffset.valueOffset, buffer1 + keyOffsetPos1 + sizeof(int64_t), sizeof(int));
                keys1.push_back(keyOffset);
                keyOffsetPos1 += sizeof(int64_t) + sizeof(int);
            }

            keyIndex1 = 0;
            currentPage1++;
        }

        // Reload page from SST2 if keys2 are exhausted and more pages are available
        if (keyIndex2 >= keys2.size() && (currentPage2 + 1) < numpages2)
        {
            offset2 += PAGE_SIZE;
            bytesRead2 = pread(sst2_fd, buffer2, PAGE_SIZE, offset2);

            if (bytesRead2 != PAGE_SIZE)
            {
                close(sst1_fd);
                close(sst2_fd);
                throw std::runtime_error("Failed to read a page from SST2.");
            }

            std::memcpy(&numEntries2, buffer2, sizeof(int)); // Update numEntries for new page
            keys2.clear();

            keyOffsetPos2 = sizeof(int) + sizeof(int64_t) + sizeof(int);
            for (int i = 0; i < numEntries2; ++i)
            {
                if (keyOffsetPos2 + sizeof(int64_t) + sizeof(int) > PAGE_SIZE)
                {
                    close(sst1_fd);
                    close(sst2_fd);
                    throw std::runtime_error("KeyOffsetPos2 exceeds PAGE_SIZE during page reload.");
                }

                KeyOffset keyOffset;
                std::memcpy(&keyOffset.key, buffer2 + keyOffsetPos2, sizeof(int64_t));
                std::memcpy(&keyOffset.valueOffset, buffer2 + keyOffsetPos2 + sizeof(int64_t), sizeof(int));
                keys2.push_back(keyOffset);
                keyOffsetPos2 += sizeof(int64_t) + sizeof(int);
            }

            keyIndex2 = 0;
            currentPage2++;
        }

        // Determine the current keys to compare
        int64_t key1 = (keyIndex1 < keys1.size()) ? keys1[keyIndex1].key : INT64_MAX;
        int64_t key2 = (keyIndex2 < keys2.size()) ? keys2[keyIndex2].key : INT64_MAX;

        std::cout << "Comparing keys: key1 = " << key1 << ", key2 = " << key2 << std::endl;

        // Compare keys and merge
        if (key1 < key2)
        {
            if (keyIndex1 >= keys1.size())
            {
                break; // Or handle appropriately
            }

            int valueOffset = keys1[keyIndex1].valueOffset;

            // Validate valueOffset within the page
            if (valueOffset < 0 || static_cast<size_t>(valueOffset) + sizeof(int64_t) > PAGE_SIZE)
            {
                close(sst1_fd);
                close(sst2_fd);
                throw std::runtime_error("Invalid valueOffset in SST1.");
            }

            int64_t value;
            std::memcpy(&value, buffer1 + valueOffset, sizeof(int64_t));

            std::cout << "[DEBUG] Processing key " << key1 << " from SST1 with value " << value << std::endl;

            if (!outputPage.addEntry(key1, value))
            {
                mergedSST->addPage(outputPage);
                outputPage = Page();
                outputPage.addEntry(key1, value);
            }

            keyIndex1++;
        }
        else if (key1 > key2)
        {
            if (keyIndex2 >= keys2.size())
            {
                break; // Or handle appropriately
            }

            int valueOffset = keys2[keyIndex2].valueOffset;

            // Validate valueOffset within the page
            if (valueOffset < 0 || static_cast<size_t>(valueOffset) + sizeof(int64_t) > PAGE_SIZE)
            {
                close(sst1_fd);
                close(sst2_fd);
                throw std::runtime_error("Invalid valueOffset in SST2.");
            }

            int64_t value;
            std::memcpy(&value, buffer2 + valueOffset, sizeof(int64_t));

            std::cout << "[DEBUG] Processing key " << key2 << " from SST2 with value " << value << std::endl;

            if (!outputPage.addEntry(key2, value))
            {
                mergedSST->addPage(outputPage);
                outputPage = Page();
                outputPage.addEntry(key2, value);
            }

            keyIndex2++;
        }
        else
        {
            if (keyIndex1 >= keys1.size() || keyIndex2 >= keys2.size())
            {
                break; // Or handle appropriately
            }

            // Retrieve values from both SSTs
            int valueOffset2 = keys2[keyIndex2].valueOffset;
            int64_t value2;
            std::memcpy(&value2, buffer2 + valueOffset2, sizeof(int64_t));

            std::cout << "[DEBUG] Processing duplicate key " << key1 << " from SST2 with value " << value2 << std::endl;
            if (!isLargestLevel || (isLargestLevel && value2 != TOMBSTONE))
            {
                if (!outputPage.addEntry(key1, value2))
                {
                    mergedSST->addPage(outputPage);
                    outputPage = Page();
                    outputPage.addEntry(key1, value2);
                }
                // If value2 is TOMBSTONE and isLargestLevel, do not add the key
            }

            // Increment both indices
            keyIndex1++;
            keyIndex2++;
        }
    }

    // Add the final page if it has entries
    if (outputPage.numEntries > 0)
    {
        mergedSST->addPage(outputPage);
    }

    // Close file descriptors
    close(sst1_fd);
    close(sst2_fd);

    return mergedSST;
}