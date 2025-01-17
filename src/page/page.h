#ifndef PAGE_H
#define PAGE_H

#include <vector>
#include <cstdint>
#include <string>
#include "globals.h"

struct KeyOffset
{
    int64_t key;
    int valueOffset;
};

class Page
{
public:
    Page();

    // Methods
    bool addEntry(int64_t key, int64_t value);
    int64_t readValueAtOffset(int valueOffset);

    // Params
    int numEntries = 0;      // Number of key-value pairs
    int64_t startingKey = 0; // Starting key for the page
    int freeSpace;           // Available space in the page

    std::vector<KeyOffset> keys; // Vector of keys and offsets within the page
    std::vector<char> data;      // Raw data representing the page contents

private:
    int calculateMetadataSize() const; // Calculates and returns the metadata size
};

#endif