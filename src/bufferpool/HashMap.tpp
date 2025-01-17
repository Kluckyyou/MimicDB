// HashMap.tpp
#include "HashMap.h"

// Constructor: Initializes the HashMap with a given capacity.
// It sets the initial size to 0 and resizes the table to hold nullptrs.
template <typename K, typename V>
HashMap<K, V>::HashMap(size_t capacity) : capacity(capacity), size(0) {
    table.resize(capacity, nullptr); // Initialize the hash table with nullptrs
}

// Insert method: Adds a key-value pair to the HashMap.
// If the key already exists, it updates the value.
// Handles collisions using separate chaining (linked lists).
template <typename K, typename V>
void HashMap<K, V>::insert(const K& key, const V& value) {
    size_t index = hashFunction(key); // Compute the hash index for the key
    HashNode<K, V>* newNode = new HashNode<K, V>(key, value); // Create a new node

    if (!table[index]) { // If the bucket is empty, insert the new node
        table[index] = newNode;
    } else {
        // Traverse the linked list to check if the key already exists
        HashNode<K, V>* prev = nullptr;
        HashNode<K, V>* current = table[index];
        while (current) {
            if (current->key == key) { // Key found, update the value
                current->value = value;
                delete newNode; // Delete the newly created node as it's not needed
                return;
            }
            prev = current;
            current = current->next;
        }
        // Key not found, append the new node to the end of the list
        prev->next = newNode;
    }
    ++size; // Increment the size of the HashMap
}

// Get method: Retrieves the value associated with a given key.
// Returns a pointer to the value if found, otherwise nullptr.
template <typename K, typename V>
V* HashMap<K, V>::get(const K& key) {
    size_t index = hashFunction(key); // Compute the hash index for the key
    HashNode<K, V>* node = table[index]; // Get the head of the linked list at the index

    // Traverse the linked list to find the key
    while (node) {
        if (node->key == key) { // Key found
            return &node->value; // Return a pointer to the value
        }
        node = node->next; // Move to the next node
    }
    return nullptr; // Key not found
}

// Remove method: Deletes the key-value pair associated with a given key.
// If the key is found, it removes the node from the linked list and deallocates memory.
template <typename K, typename V>
void HashMap<K, V>::remove(const K& key) {
    size_t index = hashFunction(key); // Compute the hash index for the key
    HashNode<K, V>* node = table[index]; // Get the head of the linked list at the index
    HashNode<K, V>* prev = nullptr; // Pointer to keep track of the previous node

    // Traverse the linked list to find the key
    while (node) {
        if (node->key == key) { // Key found
            if (prev) {
                prev->next = node->next; // Bypass the current node
            } else {
                table[index] = node->next; // Update the head if the node to remove is the first node
            }
            delete node; // Deallocate memory for the removed node
            --size; // Decrement the size of the HashMap
            return;
        }
        prev = node; // Update the previous node
        node = node->next; // Move to the next node
    }
    // If the key is not found, do nothing
}

// Destructor: Cleans up all dynamically allocated memory.
// It iterates through each bucket and deletes all nodes in the linked lists.
template <typename K, typename V>
HashMap<K, V>::~HashMap() {
    for (auto& node : table) { // Iterate over each bucket in the hash table
        while (node) { // Traverse the linked list in the current bucket
            HashNode<K, V>* temp = node; // Temporary pointer to the current node
            node = node->next; // Move to the next node
            delete temp; // Delete the current node
        }
    }
}
