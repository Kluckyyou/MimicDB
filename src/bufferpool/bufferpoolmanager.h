#ifndef BUFFER_POOL_MANAGER_H
#define BUFFER_POOL_MANAGER_H

#include "bufferpool.h"

// The BufferPoolManager class provides a singleton interface to manage
// a single instance of a BufferPool. This ensures that only one instance
// of the BufferPool exists in the program, preventing accidental duplication
// and ensuring consistent management of resources.
class BufferPoolManager {
public:
    // Static method to access the singleton instance of BufferPool.
    // The BufferPool instance is lazily initialized with a capacity of 1024.
    static BufferPool& getInstance() {
        static BufferPool instance(1024); // Specify the buffer pool capacity
        return instance;
    }

    // Deleted copy constructor to prevent copying of the BufferPoolManager instance.
    BufferPoolManager(const BufferPoolManager&) = delete;

    // Deleted assignment operator to prevent assignment of the BufferPoolManager instance.
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;

private:
    // Private constructor ensures that no instances of BufferPoolManager
    // can be created outside this class.
    BufferPoolManager() {}
};

#endif
