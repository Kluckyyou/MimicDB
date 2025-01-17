#include "page.h"
#include <cstring>
#include <iostream>

Page::Page() : data(PAGE_SIZE, 0)
{
    int metadataSize = calculateMetadataSize();
    freeSpace = PAGE_SIZE - metadataSize;
}

bool Page::addEntry(int64_t key, int64_t value)
{
    int entrySize = sizeof(key) + sizeof(int) + sizeof(value);

    if (entrySize > freeSpace)
    {
        return false; // Not enough space for this entry
    }

    if (numEntries == 0)
    {
        startingKey = key;
    }

    int valueOffset = PAGE_SIZE - sizeof(value) * (numEntries + 1);

    // Add the key-offset pair to the `keys` vector
    keys.push_back({key, valueOffset});

    // Copy the value into the `data` vector at the calculated offset
    std::memcpy(&data[valueOffset], &value, sizeof(value));

    // **Increment `numEntries` before serializing the metadata**
    ++numEntries;

    freeSpace -= entrySize;

    // Update the metadata in the `data` vector
    size_t metadataOffset = 0;
    std::memcpy(&data[metadataOffset], &numEntries, sizeof(numEntries));
    metadataOffset += sizeof(numEntries);

    std::memcpy(&data[metadataOffset], &startingKey, sizeof(startingKey));
    metadataOffset += sizeof(startingKey);

    std::memcpy(&data[metadataOffset], &freeSpace, sizeof(freeSpace));
    metadataOffset += sizeof(freeSpace);

    // Copy the keys vector into the `data` vector
    for (const auto &keyOffset : keys)
    {
        std::memcpy(&data[metadataOffset], &keyOffset.key, sizeof(keyOffset.key));
        metadataOffset += sizeof(keyOffset.key);

        std::memcpy(&data[metadataOffset], &keyOffset.valueOffset, sizeof(keyOffset.valueOffset));
        metadataOffset += sizeof(keyOffset.valueOffset);
    }

       return true;
}

int64_t Page::readValueAtOffset(int valueOffset)
{
    if (valueOffset < 0 || valueOffset + sizeof(int64_t) > PAGE_SIZE)
    {
        throw std::out_of_range("Invalid value offset in page");
    }

    const char *valuePtr = &data[valueOffset];

    // Cast the pointer to an int64_t pointer and dereference it to retrieve the value
    return *reinterpret_cast<const int64_t *>(valuePtr);
}

int Page::calculateMetadataSize() const
{
    return sizeof(numEntries) + sizeof(startingKey) + sizeof(int);
}