#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <vector>
#include <string>
#include <functional>
#include "murmur3.h"

class BloomFilter {
private:
    std::vector<bool> bitArray;  // Bit array to store the Bloom filter bits

public:
    int numBits;                // Total number of bits in the Bloom filter
    int numHashFunctions;       // Number of hash functions
    std::vector<char> data;
    void updateData();  // Function to update the data field
    void printData();
    // Constructor
    BloomFilter(int entries, int bitsPerEntry);

    // Insert a key into the Bloom filter
    void insert(int64_t key);

    // Query a key in the Bloom filter
    bool query(int64_t key) const;

    // Helper function to generate k independent hash values
    std::vector<int> getHashValues(int64_t key) const;
    size_t hashFunction(int64_t key) const;
};

#endif // BLOOMFILTER_H

