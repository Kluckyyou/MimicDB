#include "bloomfilter.h"
#include <cmath>
#include <iostream>
#include "global/globals.h"

// Constructor
BloomFilter::BloomFilter(int entries, int bitsPerEntry)
    : numBits(PAGE_SIZE),
      numHashFunctions(std::max(1, static_cast<int>(std::round(bitsPerEntry * std::log(2))))) {
    bitArray.resize(numBits, false);
}

// Helper function to generate hash values
std::vector<int> BloomFilter::getHashValues(int64_t key) const {
    std::vector<int> hashes(numHashFunctions);
    size_t hash1 = hashFunction(key);                // Primary hash
    size_t hash2 = std::hash<size_t>{}(hash1);       // Secondary hash
    for (int i = 0; i < numHashFunctions; ++i) {
        hashes[i] = (hash1 + i * hash2) % numBits;   // Double hashing
    }
    return hashes;
}

// Insert a key into the Bloom filter
void BloomFilter::insert(int64_t key) {
    for (int hash : getHashValues(key)) {
        bitArray[hash] = true;
    }
}

// Query a key in the Bloom filter
bool BloomFilter::query(int64_t key) const {
    for (int hash : getHashValues(key)) {
        if (!bitArray[hash]) {
            return false; // Abort on first zero
        }
    }
    return true;
}


void BloomFilter::updateData() {
    // Calculate the required size for the data vector (bitArray.size() rounded up to the nearest byte)
    size_t byteSize = PAGE_SIZE; // Each byte holds 8 bits
    data.resize(byteSize);

    // Initialize all bytes in data to 0
    std::fill(data.begin(), data.end(), 0);

    // Pack bits from bitArray into data
    for (size_t i = 0; i < bitArray.size(); ++i) {
        if (bitArray[i]) {
            data[i] = 1; // Set the corresponding bit in the byte
        }
    }
}


void BloomFilter::printData() {
    std::cout << "Data field (size = " << data.size() << "): ";
    for (size_t i = 0; i < data.size(); ++i) {
        // Print each byte in hexadecimal format for better readability
        printf("%02X ", static_cast<unsigned char>(data[i]));
    }
    std::cout << std::endl;
}

size_t BloomFilter::hashFunction(int64_t key) const {
    // A simple but effective hash combining shifts and modular arithmetic
    key = ~key + (key << 21); // Key modification
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // Another modification step
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // More mixing
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return static_cast<size_t>(key) % numBits; // Ensure result fits within the bit array
};