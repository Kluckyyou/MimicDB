#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include "HashMap.h" // Custom hash map implementation for page storage
#include <string>    // For using std::string
#include "globals.h" // Global configurations/constants (if any)
#include "page.h"    // Page structure definition

// Structure to represent a node in the clock replacement policy's circular doubly linked list.
struct ClockNode
{
    std::string pageID; // The ID of the page this node represents.
    int referenceBit;   // Reference bit for the clock replacement policy.
    ClockNode *next;    // Pointer to the next node in the list.
    ClockNode *prev;    // Pointer to the previous node in the list.

    // Constructor to initialize the clock node with the given page ID.
    ClockNode(const std::string &id) : pageID(id), referenceBit(0), next(nullptr), prev(nullptr) {}
};

// BufferPool class manages a fixed-size pool of pages in memory.
// Implements the clock replacement policy to handle evictions when the pool is full.
class BufferPool
{
private:
    HashMap<std::string, std::pair<Page, ClockNode *>> pageMap; // Hash map for fast page lookups.
    ClockNode *clockHand; // Pointer to the current position of the clock hand.
    ClockNode *head;      // Pointer to the head of the circular doubly linked list.
    size_t capacity;      // Maximum number of pages the buffer pool can hold.
    size_t currentSize;   // Current number of pages in the buffer pool.

    // Private helper method to evict a page using the clock replacement policy.
    void evictPage();

public:
    // Constructor to initialize the buffer pool with a given capacity.
    BufferPool(size_t capacity);

    // Retrieves a page from the buffer pool based on its ID.
    // Returns a pointer to the page if it exists, otherwise nullptr.
    Page *getPage(const std::string &pageID);

    // Inserts a page into the buffer pool.
    // If the page already exists, updates its reference bit.
    // If the buffer pool is full, evicts a page before inserting.
    void insertPage(const std::string &pageID, const Page &page);

    // Destructor to clean up all dynamically allocated memory.
    ~BufferPool();
};

#endif // BUFFERPOOL_H
