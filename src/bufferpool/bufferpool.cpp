#include "bufferpool.h"

// Constructor: Initializes the BufferPool with the specified capacity.
// Sets the current size to 0, initializes the page map, and prepares for the clock eviction policy.
BufferPool::BufferPool(size_t capacity) : capacity(capacity), currentSize(0), clockHand(nullptr), head(nullptr)
{
    // Initialize the pageMap with the specified capacity.
    pageMap = HashMap<std::string, std::pair<Page, ClockNode *>>(capacity);
}

// Get method: Retrieves a page from the buffer pool based on its page ID.
// If the page exists, updates its reference bit for the clock replacement policy.
Page *BufferPool::getPage(const std::string &pageID)
{
    // Attempt to find the page in the map.
    auto *nodePair = pageMap.get(pageID);
    if (!nodePair)
    {
        return nullptr; // Page not found.
    }
    // Mark the page as recently accessed by setting the reference bit.
    nodePair->second->referenceBit = 1;
    return &nodePair->first;
}

// Insert method: Adds a page to the buffer pool.
// If the buffer pool is full, evicts a page using the clock replacement policy.
void BufferPool::insertPage(const std::string &pageID, const Page &page)
{
    // Check if the page is already in the buffer pool.
    if (pageMap.get(pageID))
    {
        // Update the reference bit if the page exists.
        auto &nodePair = *pageMap.get(pageID);
        nodePair.second->referenceBit = 1;
        return;
    }

    // Evict a page if the buffer pool is full.
    if (currentSize == capacity)
    {
        evictPage();
    }

    // Create a new clock node for the page.
    ClockNode *newNode = new ClockNode(pageID);
    pageMap.insert(pageID, {page, newNode});

    // Add the new node to the circular doubly linked list.
    if (!head)
    {
        // If the list is empty, the new node becomes the only node in the list.
        head = newNode;
        newNode->next = newNode;
        newNode->prev = newNode;
    }
    else
    {
        // Insert the new node at the end of the list.
        ClockNode *tail = head->prev;
        tail->next = newNode;
        newNode->prev = tail;
        newNode->next = head;
        head->prev = newNode;
    }

    // If the clock hand is not set, point it to the head of the list.
    if (!clockHand)
    {
        clockHand = head;
    }

    ++currentSize; // Increment the size of the buffer pool.
}

// Eviction method: Removes a page from the buffer pool based on the clock replacement policy.
void BufferPool::evictPage()
{
    while (true)
    {
        // Check the reference bit of the page pointed to by the clock hand.
        if (clockHand->referenceBit == 0)
        {
            // Evict the page and remove it from the map.
            pageMap.remove(clockHand->pageID);

            // Remove the node from the circular linked list.
            ClockNode *toEvict = clockHand;
            if (clockHand->next == clockHand)
            {
                // If the list has only one node, reset the head and clock hand.
                head = nullptr;
                clockHand = nullptr;
            }
            else
            {
                // Update the links of the previous and next nodes.
                ClockNode *prevNode = clockHand->prev;
                ClockNode *nextNode = clockHand->next;
                prevNode->next = nextNode;
                nextNode->prev = prevNode;
                if (head == clockHand)
                {
                    head = nextNode; // Update the head if the evicted node was the head.
                }
                clockHand = nextNode; // Move the clock hand to the next node.
            }

            delete toEvict; // Deallocate the evicted node.
            --currentSize; // Decrement the size of the buffer pool.
            return;
        }
        else
        {
            // Clear the reference bit and move the clock hand to the next node.
            clockHand->referenceBit = 0;
            clockHand = clockHand->next;
        }
    }
}

// Destructor: Cleans up the buffer pool, ensuring all dynamically allocated memory is freed.
BufferPool::~BufferPool()
{
    if (!head)
        return; // If the list is empty, nothing to clean up.

    // Detect if the list is circular using Floyd's cycle detection algorithm.
    ClockNode *slow = head;
    ClockNode *fast = head->next;
    bool isCircular = false;

    while (fast && fast != slow)
    {
        slow = slow->next;
        fast = fast->next ? fast->next->next : nullptr;
    }

    if (fast == slow)
    {
        isCircular = true; // The list is circular.
    }

    if (isCircular)
    {
        // Break the cycle by setting the last node's next pointer to nullptr.
        slow = head;
        while (slow->next != head)
        {
            slow = slow->next;
        }
        slow->next = nullptr;
    }

    // Safely delete all nodes in the list.
    while (head)
    {
        ClockNode *nextNode = head->next;
        delete head;
        head = nextNode;
    }
}
