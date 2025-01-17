#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <vector>
#include <cstdint>

// Node structure for AVL Tree
struct Node
{
    int64_t key;
    int64_t value;
    Node *left;
    Node *right;
    int height;

    Node(int64_t k, int64_t v); // Constructor updated to int64_t
};

// AVL Tree class that supports put and get operations
class AVLTree
{
private:
    Node *root;
    int memtable_size; // Threshold for AVL tree size
    int current_size;  // Current AVL tree size

    // Helper functions for tree manipulation
    int height(Node *node);
    int updateHeight(Node *node);
    int balanceFactor(Node *node);
    Node *rotateRight(Node *y);
    Node *rotateLeft(Node *x);
    Node *balance(Node *node);

    // Recursive functions for put and get
    Node *put(Node *node, int64_t key, int64_t value);
    int64_t get(Node *node, int64_t key);

    // Recursive in-order traversal for scan
    void inOrderTraversal(Node *node, std::vector<std::pair<int64_t, int64_t>> &result, int64_t start, int64_t end); // Updated to int64_t

    // Recursive function to clear the tree after it is full
    void clearTree(Node *node);

public:
    AVLTree(int max_size); // Constructor

    void put(int64_t key, int64_t value);

    void del(int64_t key);

    int64_t get(int64_t key);

    // Scan method to get all key-value pairs in the range [start, end] or all by default
    std::vector<std::pair<int64_t, int64_t>> scan(int64_t start, int64_t end);

    // Clear the AVL tree
    void clear();

    // Get the current size of the AVL tree
    int getCurrentSize() const;
};

#endif