#include "kvstore.h"
#include "globals.h"
#include "sst/sst.h"
#include <unordered_set>
#include <algorithm>   // For std::sort, std::unique
#include <iostream>    // For std::cout
#include <filesystem>  // For filesystem operations
#include <fstream>     // For file stream operations (ifstream, ofstream)
#include <fcntl.h>     // For open, O_RDONLY, O_WRONLY, O_CREAT
#include <unistd.h>    // For pread, pwrite, close
#include <stdexcept>   // For runtime_error
#include <climits>     // For INT_MIN, INT_MAX
#include <sys/types.h> // For file size types like off_t
#include <cstring>     // For C string functions like strerror
#include <vector>
#include <string> // For std::string
#include <cmath>
#include "bloomfilter.h"
#include "bufferpool.h"
#include "bufferpoolmanager.h"

// Constructor
KVStore::KVStore(int memtable_size, size_t levelSizeRatio)
    : memtable(memtable_size), memtable_size(memtable_size), sst_counter(0)
{
}

void KVStore::SetUseBTree(bool flag)
{
    useBTree = flag;
}

void KVStore::Open(const std::string &database_name)
{
    db_name = "../" + database_name;
    lsmTree = std::make_unique<LSMTree>(db_name, 2);
    sst_counter = 0;

    if (!std::filesystem::exists(db_name))
    {
        // **New Database**
        if (!std::filesystem::create_directory(db_name))
        {
            throw std::runtime_error("Failed to create database directory: " + db_name);
        }
        std::cout << "Created new database directory: " << db_name << std::endl;

        // Initialize LSMTree without existing SSTs
        lsmTree = std::make_unique<LSMTree>(db_name, 2); // Assuming '2' is the number of levels
    }
    else
    {
        // **Existing Database**
        std::string metadata_path = db_name + "/lsmtree.log";

        // Initialize LSMTree
        lsmTree = std::make_unique<LSMTree>(db_name, 2);

        // Check if lsmtree.log exists
        if (std::filesystem::exists(metadata_path))
        {
            std::ifstream meta_file(metadata_path);
            if (!meta_file.is_open())
            {
                throw std::runtime_error("Failed to open metadata log: " + metadata_path);
            }

            std::string line;
            bool counter_read = false;
            while (std::getline(meta_file, line))
            {
                std::stringstream ss(line);
                std::string first_field, second_field;

                if (!std::getline(ss, first_field, ','))
                    continue;

                if (!counter_read)
                {
                    // Read the counter from the first line
                    if (first_field == "counter")
                    {
                        if (!std::getline(ss, second_field, ','))
                            continue;
                        sst_counter = std::stoul(second_field);
                        counter_read = true;
                        continue;
                    }
                }

                // Read level and sst_filename
                std::string level_str = first_field;
                if (!std::getline(ss, second_field, ','))
                    continue;

                size_t level = std::stoul(level_str);
                std::string sst_filename = second_field;

                // Add SST to the specified level
                lsmTree->addSSTToLevel(sst_filename, level);
            }
            meta_file.close();
            std::cout << "Reconstructed LSMTree from metadata log." << std::endl;
        }
        else
        {
            std::cout << "Metadata log not found. Initializing empty LSMTree." << std::endl;
        }
    }

    memtable.clear();
}

void KVStore::Put(int64_t key, int64_t value)
{
    // Insert the key-value pair into the AVLTree (memtable)
    memtable.put(key, value);

    // Check if the memtable has reached its size limit
    if (memtable.getCurrentSize() >= memtable_size)
    {
        flushMemtableToSST();
        memtable.clear();
    }
}

void KVStore::Del(int64_t key)
{
    Put(key, TOMBSTONE);
}

void KVStore::Close()
{
    // Flush the memtable to SST if it is not empty
    if (memtable.getCurrentSize() > 0)
    {
        flushMemtableToSST();
    }

    // Gather all SST filenames from the LSM Tree
    std::vector<std::vector<std::string>> levels;
    size_t num_levels = lsmTree->getNumLevels();
    for (size_t level = 0; level < num_levels; ++level)
    {
        levels.emplace_back(lsmTree->getSSTFilesByLevel(level));
    }

    // Write metadata to a temporary log file
    std::string metadata_path = db_name + "/lsmtree.log";
    std::string temp_metadata_path = metadata_path + ".tmp";
    std::ofstream meta_file(temp_metadata_path, std::ios::out | std::ios::trunc);
    if (!meta_file.is_open())
    {
        throw std::runtime_error("Failed to open temporary metadata log for writing.");
    }

    meta_file << "counter," << sst_counter << "\n";

    for (size_t level = 0; level < levels.size(); ++level)
    {
        for (const auto &sst_filename : levels[level])
        {
            meta_file << level << "," << sst_filename << "\n";
        }
    }
    meta_file.close();

    // Atomically replace the old metadata log with the new one
    std::filesystem::rename(temp_metadata_path, metadata_path);
    std::cout << "Metadata log updated at: " << metadata_path << std::endl;

    // Clear the memtable and SST filenames
    memtable.clear();
    lsmTree->clearLevels();
}

int64_t KVStore::Get(int64_t key)
{
    // Step 1: Check in the memtable first
    int64_t result = memtable.get(key);
    if (result != -1) // Assuming -1 indicates "not found" in memtable
    {
        if (result == TOMBSTONE)
        {
            std::cout << "DEBUG: Key " << key << " is a tombstone." << std::endl;
            return -1;
        }
        else
        {
            std::cout << "DEBUG: Found key " << key << " in memtable with value: " << result << std::endl;
            return result;
        }
    }

    std::cout << "DEBUG: Key " << key << " not found in memtable. Searching SST files in LSM Tree..." << std::endl;

    // Step 2: Search the LSM Tree level by level
    for (size_t level = 0; level < lsmTree->getNumLevels(); ++level)
    {
        auto sstFiles = lsmTree->getSSTFilesByLevel(level);
        std::cout << "DEBUG: Searching in Level " << level << " with " << sstFiles.size() << " SST files..." << std::endl;

        for (const auto &sst_filename : sstFiles)
        {
            std::cout << "DEBUG: Searching key " << key << " in SST file: " << sst_filename << std::endl;

            int sst_fd = open(sst_filename.c_str(), O_RDONLY);
            if (sst_fd == -1)
            {
                std::cerr << "ERROR: Failed to open SST file for reading: " << sst_filename << std::endl;
                continue; // Skip this SST file
            }

            BloomFilter bloom = BloomFilter(NUM_ENTRIES, BITS_PER_ENTRY);
            char bloom_buffer[PAGE_SIZE];

            off_t bloom_offset = SST_METADATA_SIZE;
            bool found = true;

            // Read the bitVector
            ssize_t bytes_read = pread(sst_fd, bloom_buffer, PAGE_SIZE, bloom_offset);
            if (bytes_read == -1 || bytes_read != PAGE_SIZE)
            {
                close(sst_fd);
                throw std::runtime_error("Failed to read bitVector.");
            }
            for (int hash : bloom.getHashValues(key))
            {
                if (!bloom_buffer[hash])
                {
                    std::cout << "DEBUG: Key " << key << " not found in using Bloom Filter." << std::endl;
                    found = false;
                    break; // Abort on first zero
                }
            }
            if (!found)
            {
                close(sst_fd);
                continue;
            }

            // Perform a binary search or B-Tree search on this SST file
            if (useBTree)
            {
                result = btreeSearchSST(sst_fd, key, sst_filename);
            }
            else
            {
                result = binarySearchSST(sst_fd, sst_filename, key);
            }

            close(sst_fd);

            if (result != -1) // Check if the key was found
            {

                if (result == TOMBSTONE)
                {
                    std::cout << "DEBUG: Key " << key << " is a tombstone." << std::endl;
                    return -1;
                }
                else
                {
                    std::cout << "DEBUG: Found key " << key << " in SST file: " << sst_filename
                              << " with value: " << result << std::endl;
                    return result; // Return the found value if key is found in this SST
                }
            }
            else
            {
                std::cout << "DEBUG: Key " << key << " not found in SST file: " << sst_filename << std::endl;
            }
        }
    }

    std::cout << "DEBUG: Key " << key << " not found in any SST file in LSM Tree. Returning -1." << std::endl;
    return -1; // Return -1 if the key was not found in memtable or any SST file
}

std::pair<int64_t, int64_t> *KVStore::Scan(int64_t start, int64_t end, int &result_count)
{

    // Perform the merged scan, which includes memtable and SSTs
    std::vector<std::pair<int64_t, int64_t>> scan_results = mergedScan(start, end);

    // Set the result count
    result_count = scan_results.size();

    // Allocate a dynamic array to hold the results
    if (result_count == 0)
    {
        return nullptr; // Or handle as per your API design
    }

    auto *results_array = new std::pair<int64_t, int64_t>[result_count];
    std::copy(scan_results.begin(), scan_results.end(), results_array);

    return results_array;
}

void KVStore::flushMemtableToSST()
{
    SST sst;
    auto kv_pairs = memtable.scan(INT_MIN, INT_MAX);

    // Set SST starting and ending keys
    sst.startingKey = kv_pairs.front().first;
    sst.endingKey = kv_pairs.back().first;

    Page currentPage;
    for (const auto &kv : kv_pairs)
    {
        int64_t key = kv.first;
        int64_t value = kv.second;

        // Attempt to add entry to current page
        if (!currentPage.addEntry(key, value))
        {
            // If the page is full, add it to the SST and start a new page
            sst.addPage(currentPage);
            currentPage = Page();             // Create a new page
            currentPage.addEntry(key, value); // Add the entry to the new page
        }
    }

    // Add the last page if it has entries
    if (currentPage.numEntries > 0)
    {
        sst.addPage(currentPage);
    }

    // Define file path and write to file
    std::string sst_filename = db_name + "/sst_" + std::to_string(++sst_counter) + ".sst";
    sst.writeToFile(sst_filename);

    // Update LSMTree with the new SST filename and trigger compaction if needed
    lsmTree->addSST(sst_filename);

    // Clear the memtable
    memtable.clear();
}

int64_t KVStore::binarySearchSST(int sst_fd, const std::string &sst_filename, int64_t target_key)
{

    // Read SST metadata
    int num_pages;
    off_t offset = sizeof(int); // Skip `numEntries` in SST metadata
    pread(sst_fd, &num_pages, sizeof(num_pages), offset);

    if (num_pages <= 0)
    {
        throw std::runtime_error("SST file has no pages.");
    }

    // Binary search the pages
    int left = 0, right = num_pages - 1;
    char page_buffer[PAGE_SIZE];
    while (left <= right)
    {
        int mid = left + (right - left) / 2;
        off_t page_offset = SST_METADATA_SIZE + PAGE_SIZE + (mid * PAGE_SIZE);

        std::string pageID = sst_filename + ":" + std::to_string(page_offset);

        // Check if the page is in the buffer pool
        BufferPool &bufferPool = BufferPoolManager::getInstance();
        Page *cachedPage = bufferPool.getPage(pageID);
        char page_buffer[PAGE_SIZE];

        if (cachedPage)
        {
            std::cout << "Buffer pool accessed" << std::endl;
            // Load data from the buffer pool
            std::memcpy(page_buffer, cachedPage->data.data(), PAGE_SIZE);
        }
        else
        {
            // Read the page
            ssize_t bytes_read = pread(sst_fd, page_buffer, PAGE_SIZE, page_offset);
            if (bytes_read == -1 || bytes_read != PAGE_SIZE)
            {
                throw std::runtime_error("Failed to read page.");
            }

            Page page;
            page.data.assign(page_buffer, page_buffer + PAGE_SIZE); // Populate page data
            bufferPool.insertPage(pageID, page);                    // Insert into buffer pool
        }

        // Deserialize page metadata
        int page_num_entries;
        int64_t page_starting_key;
        std::memcpy(&page_num_entries, page_buffer, sizeof(int));
        std::memcpy(&page_starting_key, page_buffer + sizeof(int), sizeof(int64_t));

        int64_t next_page_starting_key = INT64_MAX;
        if (mid < num_pages - 1)
        {
            off_t next_page_offset = SST_METADATA_SIZE + PAGE_SIZE + ((mid + 1) * PAGE_SIZE);
            pread(sst_fd, &next_page_starting_key, sizeof(int64_t), next_page_offset + sizeof(int));
        }

        if (target_key < page_starting_key)
        {
            right = mid - 1;
        }
        else if (target_key < next_page_starting_key)
        {
            // Process keys in this page
            size_t offset_in_page = sizeof(int) + sizeof(int64_t) + sizeof(int); // Skip metadata

            // Binary search within the key-offset vector
            int low = 0, high = page_num_entries; // `page_num_entries` includes all entries in the page
            while (low <= high)
            {
                int mid = low + (high - low) / 2;
                size_t key_offset_position = offset_in_page + mid * (sizeof(int64_t) + sizeof(int));

                // Read the key and value offset at the `mid` position
                KeyOffset key_offset;
                std::memcpy(&key_offset.key, page_buffer + key_offset_position, sizeof(int64_t));
                std::memcpy(&key_offset.valueOffset, page_buffer + key_offset_position + sizeof(int64_t), sizeof(int));

                // Debugging the key being checked
                std::cout << "DEBUG: Checking key=" << key_offset.key << " at mid=" << mid << std::endl;

                if (key_offset.key == target_key)
                {
                    // Extract value using offset
                    int64_t value;
                    if (key_offset.valueOffset >= 0 && key_offset.valueOffset + sizeof(int64_t) <= PAGE_SIZE)
                    {
                        std::memcpy(&value, page_buffer + key_offset.valueOffset, sizeof(value));
                        return value;
                    }
                    else
                    {
                        throw std::runtime_error("Invalid value offset in page.");
                    }
                }
                else if (key_offset.key < target_key)
                {
                    low = mid + 1; // Move to the right
                }
                else
                {
                    high = mid - 1; // Move to the left
                }
            }
            return -1; // Key not found in this page
        }
        else
        {
            left = mid + 1;
        }
    }

    return -1; // Key not found
}

std::vector<std::pair<int64_t, int64_t>> KVStore::scanSST(int sst_fd, const std::string &sst_filename, int64_t start, int64_t end)
{
    // Vector to hold the results
    std::vector<std::pair<int64_t, int64_t>> results;

    // Read SST metadata to get the number of pages
    int num_pages;
    off_t offset = sizeof(int); // Skip numEntries in SST metadata
    pread(sst_fd, &num_pages, sizeof(num_pages), offset);

    if (num_pages <= 0)
    {
        throw std::runtime_error("SST file has no pages.");
    }

    // Perform binary search at the page level to find the starting page
    int left = 0, right = num_pages - 1;
    int starting_page = -1;
    char page_buffer[PAGE_SIZE];

    while (left <= right)
    {
        int mid = left + (right - left) / 2;
        off_t page_offset = SST_METADATA_SIZE + PAGE_SIZE + (mid * PAGE_SIZE);

        std::string pageID = sst_filename + ":" + std::to_string(page_offset);

        // Check if the page is in the buffer pool
        BufferPool &bufferPool = BufferPoolManager::getInstance();
        Page *cachedPage = bufferPool.getPage(pageID);
        char page_buffer[PAGE_SIZE];

        if (cachedPage)
        {
            std::cout << "Buffer pool accessed" << std::endl;
            // Load data from the buffer pool
            std::memcpy(page_buffer, cachedPage->data.data(), PAGE_SIZE);
        }
        else
        {
            ssize_t bytes_read = pread(sst_fd, page_buffer, PAGE_SIZE, page_offset);
            if (bytes_read == -1 || bytes_read != PAGE_SIZE)
            {
                throw std::runtime_error("Failed to read SST file or incomplete page read.");
            }

            Page page;
            page.data.assign(page_buffer, page_buffer + PAGE_SIZE); // Populate page data
            bufferPool.insertPage(pageID, page);                    // Insert into buffer pool
        }

        // Deserialize the page metadata
        int page_num_entries;
        int64_t page_starting_key;
        std::memcpy(&page_num_entries, page_buffer, sizeof(int));
        std::memcpy(&page_starting_key, page_buffer + sizeof(int), sizeof(int64_t));

        // Check if this page overlaps with the range [start, end]
        if (end < page_starting_key)
        {
            // The range ends before this page, so move left.
            right = mid - 1;
        }
        else
        {
            // This page is a potential match or overlaps with the range.
            starting_page = mid;
            left = mid + 1;
        }
    }

    // If no starting page is found, return an empty result
    if (starting_page == -1)
    {
        return results;
    }

    // Sequentially scan from the starting page onward
    for (int page = starting_page; page < num_pages; ++page)
    {
        off_t page_offset = SST_METADATA_SIZE + PAGE_SIZE + (page * PAGE_SIZE);

        std::string pageID = sst_filename + ":" + std::to_string(page_offset);

        // Check if the page is in the buffer pool
        BufferPool &bufferPool = BufferPoolManager::getInstance();
        Page *cachedPage = bufferPool.getPage(pageID);
        char page_buffer[PAGE_SIZE];

        if (cachedPage)
        {
            std::cout << "Buffer pool accessed" << std::endl;
            // Load data from the buffer pool
            std::memcpy(page_buffer, cachedPage->data.data(), PAGE_SIZE);
        }
        else
        {
            ssize_t bytes_read = pread(sst_fd, page_buffer, PAGE_SIZE, page_offset);
            if (bytes_read == -1 || bytes_read != PAGE_SIZE)
            {
                throw std::runtime_error("Failed to read SST file or incomplete page read.");
            }

            Page page;
            page.data.assign(page_buffer, page_buffer + PAGE_SIZE); // Populate page data
            bufferPool.insertPage(pageID, page);                    // Insert into buffer pool
        }

        // Deserialize the page metadata
        int page_num_entries;
        int64_t page_starting_key, page_free_space;
        std::memcpy(&page_num_entries, page_buffer, sizeof(int));
        std::memcpy(&page_starting_key, page_buffer + sizeof(int), sizeof(int64_t));
        std::memcpy(&page_free_space, page_buffer + sizeof(int) + sizeof(int64_t), sizeof(int));

        // If the page starts beyond the end of the range, stop scanning
        if (page_starting_key > end)
        {
            break;
        }

        // Iterate through the keys in the page and collect results in range [start, end]
        size_t offset_in_page = sizeof(int) + sizeof(int64_t) + sizeof(int); // Skip metadata
        for (int i = 0; i <= page_num_entries; ++i)
        {
            KeyOffset key_offset;
            std::memcpy(&key_offset.key, page_buffer + offset_in_page, sizeof(int64_t));
            std::memcpy(&key_offset.valueOffset, page_buffer + offset_in_page + sizeof(int64_t), sizeof(int));

            if (key_offset.key >= start && key_offset.key <= end)
            {
                int64_t value;
                std::memcpy(&value, page_buffer + key_offset.valueOffset, sizeof(value));
                results.emplace_back(key_offset.key, value);
            }
            else if (key_offset.key > end)
            {
                // Stop scanning further keys as they exceed the range
                return results;
            }

            offset_in_page += sizeof(int64_t) + sizeof(int); // Move to next key-offset
        }
    }

    return results;
}

int64_t KVStore::btreeSearchSST(int sst_fd, int64_t target_key, const std::string &sst_filename)
{
    BufferPool &bufferPool = BufferPoolManager::getInstance();
    // Step 1: Read SST-level metadata to get the number of pages
    int numPages = 0;
    off_t offset = sizeof(int); // Skip `numEntries` (4 bytes) in SST metadata
    pread(sst_fd, &numPages, sizeof(numPages), offset);

    if (numPages <= 0)
    {
        throw std::runtime_error("SST file has no pages.");
    }

    // Calculate the range for pages in the SST file
    off_t pageStartOffset = SST_METADATA_SIZE + PAGE_SIZE;
    off_t pageEndOffset = pageStartOffset + (numPages * PAGE_SIZE);

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Step 2: Seek to the last 4KB of the file to load the root node
    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    off_t rootOffset = lseek(sst_fd, -PAGE_SIZE, SEEK_END);
    if (rootOffset == -1)
    {
        throw std::runtime_error("Failed to seek to the last 4KB of the file");
    }
    std::string pageID = sst_filename + ":" + std::to_string(rootOffset);

    Page *cachedPage = bufferPool.getPage(pageID);
    char buffer[PAGE_SIZE];
    if (cachedPage)
    {
        std::cout << "Buffer pool accessed" << std::endl;
        // Load data from the buffer pool
        std::memcpy(buffer, cachedPage->data.data(), PAGE_SIZE);
    }
    else
    {
        // Read the root page from the SST file and insert it into the buffer pool
        ssize_t bytesRead = read(sst_fd, buffer, PAGE_SIZE);
        if (bytesRead == -1)
        {
            throw std::runtime_error("Failed to read the last 4KB of the file");
        }

        Page rootPage;
        rootPage.data.assign(buffer, buffer + PAGE_SIZE); // Populate page data
        bufferPool.insertPage(pageID, rootPage);          // Insert into buffer pool
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Step 3: Parse the buffer to search the root node
    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    int32_t keyCount = 0;
    int32_t offCount = 0;
    size_t metadataOffset = 0;

    // Read the number of keys (first 8 bytes)
    std::memcpy(&keyCount, buffer + metadataOffset, sizeof(keyCount));
    metadataOffset += sizeof(keyCount);

    if (keyCount <= 0)
    {
        throw std::runtime_error("Invalid key count in node");
    }

    // Read the number of offsets
    std::memcpy(&offCount, buffer + metadataOffset, sizeof(offCount));
    metadataOffset += sizeof(offCount);

    if (offCount <= 0)
    {
        throw std::runtime_error("Invalid off count in node");
    }

    int64_t currentOffset = 0;
    int64_t currentKey = 0;

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Step 4: Iterate over the keys and offsets to find the appropriate child
    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    for (int i = 0; i < keyCount; ++i)
    {
        // Read the offset (next 8 bytes)
        std::memcpy(&currentOffset, buffer + metadataOffset, sizeof(currentOffset));
        metadataOffset += sizeof(currentOffset);
        std::cout << "Current Offset: " << currentOffset << std::endl;

        // Read the key (next 8 bytes)
        std::memcpy(&currentKey, buffer + metadataOffset, sizeof(currentKey));
        metadataOffset += sizeof(currentKey);
        std::cout << "Current Key: " << currentKey << std::endl;

        std::cout << "Target Key: " << target_key << std::endl;
        // Compare with the target key
        if (target_key <= currentKey)
        {
            // If target_key is smaller or equal to the current key, follow the current offset
            std::cout << "Found offset, trying to follow" << std::endl;
            return followOffset(sst_fd, currentOffset, target_key, sst_filename, pageStartOffset, pageEndOffset);
        }
    }

    // If the key is greater than all keys in the current node, follow the last offset if available
    if (keyCount < offCount)
    {
        std::cout << "Should not be here" << std::endl;
        // Read the last offset
        std::memcpy(&currentOffset, buffer + metadataOffset, sizeof(currentOffset));
        metadataOffset += sizeof(currentOffset);
        return followOffset(sst_fd, currentOffset, target_key, sst_filename, pageStartOffset, pageEndOffset);
    }

    // If the key was not found, return -1 to indicate not found
    return -1;
}

int64_t KVStore::followOffset(int sst_fd, int64_t offset, int64_t target_key, const std::string &sst_filename, off_t pageStartOffset, off_t pageEndOffset)
{
    std::cout << "pageStartOffset: " << pageStartOffset << std::endl;
    std::cout << "pageEndOffset: " << pageEndOffset << std::endl;
    std::string pageID = sst_filename + ":" + std::to_string(offset);

    // Check if the page is in the buffer pool
    BufferPool &bufferPool = BufferPoolManager::getInstance();
    Page *cachedPage = bufferPool.getPage(pageID);
    char buffer[PAGE_SIZE];

    if (cachedPage)
    {
        std::cout << "Buffer pool accessed" << std::endl;
        // Load data from the buffer pool
        std::memcpy(buffer, cachedPage->data.data(), PAGE_SIZE);
    }
    else
    {
        // Read the page/node and insert it into the buffer pool
        ssize_t bytesRead = pread(sst_fd, buffer, PAGE_SIZE, offset);
        if (bytesRead == -1)
        {
            throw std::runtime_error("Failed to read the SST page or B-tree node");
        }

        Page page;
        page.data.assign(buffer, buffer + PAGE_SIZE); // Populate page data
        bufferPool.insertPage(pageID, page);          // Insert into buffer pool
    }

    if (offset >= pageStartOffset && offset < pageEndOffset)
    {
        // The offset points to an SST page; search in the page
        return searchInPage(buffer, target_key);
    }
    else
    {
        // The offset points to another B-tree node; search in the node
        return searchInNode(buffer, target_key, sst_fd, sst_filename, pageStartOffset, pageEndOffset);
    }
}

int64_t KVStore::searchInNode(char *nodeBuffer, int64_t target_key, int sst_fd, const std::string &sst_filename, off_t pageStartOffset, off_t pageEndOffset)
{
    int32_t keyCount = 0;
    int32_t offCount = 0;
    size_t metadataOffset = 0;

    // Read the number of keys
    std::memcpy(&keyCount, nodeBuffer + metadataOffset, sizeof(keyCount));
    metadataOffset += sizeof(keyCount);

    if (keyCount <= 0)
    {
        throw std::runtime_error("Invalid key count in node");
    }

    // Read the number of offsets
    std::memcpy(&offCount, nodeBuffer + metadataOffset, sizeof(offCount));
    metadataOffset += sizeof(offCount);

    if (offCount <= 0)
    {
        throw std::runtime_error("Invalid off count in node");
    }

    int64_t currentOffset = 0;
    int64_t currentKey = 0;

    // Iterate over the keys and offsets
    for (int i = 0; i < keyCount; ++i)
    {
        // Read the offset
        std::memcpy(&currentOffset, nodeBuffer + metadataOffset, sizeof(currentOffset));
        metadataOffset += sizeof(currentOffset);

        // Read the key
        std::memcpy(&currentKey, nodeBuffer + metadataOffset, sizeof(currentKey));
        metadataOffset += sizeof(currentKey);

        // Compare with the target key
        if (target_key <= currentKey)
        {
            return followOffset(sst_fd, currentOffset, target_key, sst_filename, pageStartOffset, pageEndOffset);
        }
    }

    // Follow the last offset if necessary
    if (keyCount < offCount)
    {
        std::memcpy(&currentOffset, nodeBuffer + metadataOffset, sizeof(currentOffset));
        metadataOffset += sizeof(currentOffset);
        return followOffset(sst_fd, currentOffset, target_key, sst_filename, pageStartOffset, pageEndOffset);
    }

    // Key not found
    return -1;
}

int64_t KVStore::searchInPage(const char *pageBuffer, int64_t target_key)
{
    // Step 1: Process metadata to get the number of entries in the page
    size_t offset_in_page = sizeof(int) + sizeof(int64_t) + sizeof(int); // Skip metadata (numEntries, startingKey, freeSpace)
    int page_num_entries = 0;
    std::memcpy(&page_num_entries, pageBuffer, sizeof(page_num_entries));

    if (page_num_entries <= 0)
    {
        throw std::runtime_error("Invalid number of entries in the page.");
    }

    // Step 2: Perform binary search on the key-offset pairs
    int low = 0, high = page_num_entries - 1; // Set the bounds for binary search

    while (low <= high)
    {
        int mid = low + (high - low) / 2;
        size_t key_offset_position = offset_in_page + mid * (sizeof(int64_t) + sizeof(int));

        // Read the key and value offset at the `mid` position
        KeyOffset key_offset;
        std::memcpy(&key_offset.key, pageBuffer + key_offset_position, sizeof(int64_t));
        std::memcpy(&key_offset.valueOffset, pageBuffer + key_offset_position + sizeof(int64_t), sizeof(int));

        // Debugging the key being checked
        std::cout << "DEBUG: Checking key=" << key_offset.key << " at mid=" << mid << std::endl;

        if (key_offset.key == target_key)
        {
            // Extract the value using the offset
            int64_t value;
            if (key_offset.valueOffset >= 0 && key_offset.valueOffset + sizeof(int64_t) <= PAGE_SIZE)
            {
                std::memcpy(&value, pageBuffer + key_offset.valueOffset, sizeof(value));
                return value; // Found the key and return the value associated with it
            }
            else
            {
                throw std::runtime_error("Invalid value offset in page.");
            }
        }
        else if (key_offset.key < target_key)
        {
            low = mid + 1; // Move to the right
        }
        else
        {
            high = mid - 1; // Move to the left
        }
    }

    // Step 3: If the key is not found, return -1
    return -1; // Key not found in this page
}

std::vector<std::pair<int64_t, int64_t>> KVStore::scanBtree(int sst_fd, const std::string &sst_filename, int64_t start, int64_t end)
{
    std::vector<std::pair<int64_t, int64_t>> result;

    // Step 1: Read SST metadata to get the number of pages
    int numPages = 0;
    off_t offset = sizeof(int); // Skip `numEntries` in SST metadata
    pread(sst_fd, &numPages, sizeof(numPages), offset);

    if (numPages <= 0)
    {
        throw std::runtime_error("SST file has no pages.");
    }

    // Calculate the range for pages in the SST file
    off_t pageStartOffset = SST_METADATA_SIZE + PAGE_SIZE;
    off_t pageEndOffset = pageStartOffset + (numPages * PAGE_SIZE);

    // Step 2: Seek to the last 4KB of the file to load the root node
    off_t rootOffset = lseek(sst_fd, -PAGE_SIZE, SEEK_END);
    if (rootOffset == -1)
    {
        throw std::runtime_error("Failed to seek to the last 4KB of the file");
    }

    // Step 3: Start scanning from the root node
    scanNode(sst_fd, sst_filename, rootOffset, start, end, pageStartOffset, pageEndOffset, result);

    return result;
}

void KVStore::scanNode(int sst_fd, const std::string &sst_filename, off_t offset, int64_t start, int64_t end, off_t pageStartOffset, off_t pageEndOffset, std::vector<std::pair<int64_t, int64_t>> &result)
{
    BufferPool &bufferPool = BufferPoolManager::getInstance();
    std::string pageID = sst_filename + ":" + std::to_string(offset);
    Page *cachedPage = bufferPool.getPage(pageID);
    char buffer[PAGE_SIZE];
    if (cachedPage)
    {
        std::cout << "Buffer pool accessed" << std::endl;
        // Load data from the buffer pool
        std::memcpy(buffer, cachedPage->data.data(), PAGE_SIZE);
    }
    else
    {
        ssize_t bytesRead = pread(sst_fd, buffer, PAGE_SIZE, offset);
        if (bytesRead == -1)
        {
            throw std::runtime_error("Failed to read the node from SST");
        }

        Page page;
        page.data.assign(buffer, buffer + PAGE_SIZE); // Populate page data
        bufferPool.insertPage(pageID, page);          // Insert into buffer pool
    }

    // Step 1: Read metadata to get the number of keys in the node
    size_t metadataOffset = 0;
    int32_t keyCount = 0;
    int32_t offCount = 0;

    std::memcpy(&keyCount, buffer + metadataOffset, sizeof(keyCount));
    metadataOffset += sizeof(keyCount);

    if (keyCount <= 0)
    {
        throw std::runtime_error("Invalid key count in node");
    }

    std::memcpy(&offCount, buffer + metadataOffset, sizeof(offCount));
    metadataOffset += sizeof(offCount);

    if (offCount <= 0)
    {
        throw std::runtime_error("Invalid key count in node");
    }

    int64_t currentOffset = 0;
    int64_t currentKey = 0;

    // Step 2: Iterate over keys and offsets to perform the range scan
    for (int i = 0; i < keyCount; ++i)
    {
        // Read the offset
        std::memcpy(&currentOffset, buffer + metadataOffset, sizeof(currentOffset));
        metadataOffset += sizeof(currentOffset);

        // Read the key
        std::memcpy(&currentKey, buffer + metadataOffset, sizeof(currentKey));
        metadataOffset += sizeof(currentKey);

        // If currentKey is within the range, collect results from the subtree/page
        if (currentKey >= start)
        {
            // Follow the current offset if necessary
            if (currentOffset >= pageStartOffset && currentOffset < pageEndOffset)
            {
                std::string pageID = sst_filename + ":" + std::to_string(currentOffset);
                Page *cachedPage = bufferPool.getPage(pageID);
                char pageBuffer[PAGE_SIZE];
                if (cachedPage)
                {
                    std::cout << "Buffer pool accessed" << std::endl;
                    // Load data from the buffer pool
                    std::memcpy(pageBuffer, cachedPage->data.data(), PAGE_SIZE);
                }
                else
                {
                    // If the offset points to a page, load and scan the page
                    ssize_t bytesRead = pread(sst_fd, pageBuffer, PAGE_SIZE, currentOffset);
                    if (bytesRead == -1)
                    {
                        throw std::runtime_error("Failed to read the SST page");
                    }

                    Page page;
                    page.data.assign(pageBuffer, pageBuffer + PAGE_SIZE); // Populate page data
                    bufferPool.insertPage(pageID, page);                  // Insert into buffer pool
                }
                // Scan the page to collect key-value pairs within the range
                scanPage(pageBuffer, start, end, result);
            }
            else
            {
                // If the offset points to another B-tree node, recursively scan the node
                scanNode(sst_fd, sst_filename, currentOffset, start, end, pageStartOffset, pageEndOffset, result);
            }
        }

        // If the key exceeds the end range, we can stop scanning further
        if (currentKey > end)
        {
            return;
        }
    }

    // Step 3: Follow the last offset if necessary and it's within the range
    if (keyCount < offCount)
    {
        std::memcpy(&currentOffset, buffer + metadataOffset, sizeof(currentOffset));
        if (currentOffset >= pageStartOffset && currentOffset < pageEndOffset)
        {
            std::string pageID = sst_filename + ":" + std::to_string(currentOffset);
            Page *cachedPage = bufferPool.getPage(pageID);
            char pageBuffer[PAGE_SIZE];
            if (cachedPage)
            {
                std::cout << "Buffer pool accessed" << std::endl;
                // Load data from the buffer pool
                std::memcpy(pageBuffer, cachedPage->data.data(), PAGE_SIZE);
            }
            else
            {
                // If the offset points to a page, load and scan the page
                ssize_t bytesRead = pread(sst_fd, pageBuffer, PAGE_SIZE, currentOffset);
                if (bytesRead == -1)
                {
                    throw std::runtime_error("Failed to read the SST page");
                }

                Page page;
                page.data.assign(pageBuffer, pageBuffer + PAGE_SIZE); // Populate page data
                bufferPool.insertPage(pageID, page);                  // Insert into buffer pool
            }

            // Scan the page to collect key-value pairs within the range
            scanPage(pageBuffer, start, end, result);
        }
        else
        {
            // Recursively scan the next B-tree node if it exists
            scanNode(sst_fd, sst_filename, currentOffset, start, end, pageStartOffset, pageEndOffset, result);
        }
    }
}

void KVStore::scanPage(const char *pageBuffer, int64_t start, int64_t end, std::vector<std::pair<int64_t, int64_t>> &result)
{
    // Process keys in this page
    size_t offset_in_page = sizeof(int) + sizeof(int64_t) + sizeof(int); // Skip metadata

    // Read the number of entries in the page
    int page_num_entries = 0;
    std::memcpy(&page_num_entries, pageBuffer, sizeof(page_num_entries));

    if (page_num_entries <= 0)
    {
        throw std::runtime_error("Invalid number of entries in the page.");
    }

    // Perform a range scan within the key-offset vector
    for (int i = 0; i < page_num_entries; ++i)
    {
        size_t key_offset_position = offset_in_page + i * (sizeof(int64_t) + sizeof(int));

        // Read the key and value offset at the current position
        KeyOffset key_offset;
        std::memcpy(&key_offset.key, pageBuffer + key_offset_position, sizeof(int64_t));
        std::memcpy(&key_offset.valueOffset, pageBuffer + key_offset_position + sizeof(int64_t), sizeof(int));

        if (key_offset.key >= start && key_offset.key <= end)
        {
            // Extract value using offset
            int64_t value;
            if (key_offset.valueOffset >= 0 && key_offset.valueOffset + sizeof(int64_t) <= PAGE_SIZE)
            {
                std::memcpy(&value, pageBuffer + key_offset.valueOffset, sizeof(value));
                result.emplace_back(key_offset.key, value);
            }
            else
            {
                throw std::runtime_error("Invalid value offset in page.");
            }
        }

        // If the current key exceeds the end of the range, stop scanning
        if (key_offset.key > end)
        {
            return;
        }
    }
}

std::vector<std::pair<int64_t, int64_t>> KVStore::mergedScan(int64_t start, int64_t end)
{
    std::vector<std::pair<int64_t, int64_t>> final_results;
    std::unordered_set<int64_t> seen_keys; // To track keys already processed

    // 1. Scan the memtable first
    auto memtable_results = memtable.scan(start, end);
    for (const auto &kv : memtable_results)
    {
        if (kv.second == TOMBSTONE)
        {
            // Mark the key as seen but do not include it in results
            seen_keys.insert(kv.first);
            continue;
        }

        // Include the key-value pair in the results
        final_results.emplace_back(kv.first, kv.second);
        seen_keys.insert(kv.first);
    }

    // 2. Iterate through levels from youngest (0) to oldest
    size_t numLevels = lsmTree->getNumLevels();

    for (size_t level = 0; level < numLevels; ++level)
    {
        std::vector<std::string> sstFiles = lsmTree->getSSTFilesByLevel(level);

        // Iterate through each SST file in the current level
        for (const auto &sst_filename : sstFiles)
        {
            try
            {
                // Open the SST file
                int sst_fd = open(sst_filename.c_str(), O_RDONLY);
                if (sst_fd == -1)
                {
                    std::cerr << "ERROR: Failed to open SST file: " << sst_filename << std::endl;
                    continue; // Skip this SST file
                }

                // Use the existing scanBtree function to get key-value pairs in range
                std::vector<std::pair<int64_t, int64_t>> sst_results = scanBtree(sst_fd, sst_filename, start, end);
                close(sst_fd); // Close the file descriptor after scanning

                // Iterate through the scan results
                for (const auto &kv : sst_results)
                {
                    // If key has already been processed, skip it
                    if (seen_keys.find(kv.first) != seen_keys.end())
                    {
                        continue;
                    }

                    if (kv.second == TOMBSTONE)
                    {
                        // Mark the key as seen but do not include it in results
                        seen_keys.insert(kv.first);
                        continue;
                    }

                    // Include the key-value pair in the results
                    final_results.emplace_back(kv.first, kv.second);
                    seen_keys.insert(kv.first);
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "ERROR during Scan: " << e.what() << std::endl;
                continue; // Skip this SST file on error
            }
        }
    }

    // 3. Sort the results by key in ascending order
    std::sort(final_results.begin(), final_results.end(), [](const std::pair<int64_t, int64_t> &a, const std::pair<int64_t, int64_t> &b)
              { return a.first < b.first; });

    return final_results;
}