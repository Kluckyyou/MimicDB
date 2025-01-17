#ifndef GLOBALS_H
#define GLOBALS_H

#include <cstdint>

constexpr int PAGE_SIZE = 4096;
constexpr size_t SST_METADATA_SIZE = 24;
constexpr int BUFFER_POOL_SIZE = 100;
constexpr int HASHMAP_SIZE = 2560;
constexpr int BTREE_DEGREE = 128;
constexpr int64_t TOMBSTONE = INT64_MIN + 5;
constexpr int BITS_PER_ENTRY = 12;
constexpr int NUM_ENTRIES = 340; // (4096-12)//12

#endif // GLOBALS_H