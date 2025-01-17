#include "memtable.h"
#include <algorithm> // For std::max
#include <iostream>  // For testing output
#include <climits>   // For INT_MIN, INT_MAX
#include <set>       // For std::set to track duplicate keys

// Node constructor
Node::Node(int64_t k, int64_t v)
    : key(k), value(v), left(nullptr), right(nullptr), height(1)
{
}

// AVLTree constructor
AVLTree::AVLTree(int max_size) : root(nullptr), memtable_size(max_size), current_size(0) {}

// Get the height of the node
int AVLTree::height(Node *node)
{
    return node ? node->height : 0;
}

// Update the height of the node based on its children's heights
int AVLTree::updateHeight(Node *node)
{
    return node ? 1 + std::max(height(node->left), height(node->right)) : 0;
}

// Calculate the balance factor of the node
int AVLTree::balanceFactor(Node *node)
{
    return node ? height(node->left) - height(node->right) : 0;
}

// Perform a right rotation
Node *AVLTree::rotateRight(Node *y)
{
    Node *x = y->left;
    Node *T2 = x->right;

    // Perform rotation
    x->right = y;
    y->left = T2;

    // Update heights
    y->height = updateHeight(y);
    x->height = updateHeight(x);

    return x; // New root
}

// Perform a left rotation
Node *AVLTree::rotateLeft(Node *x)
{
    Node *y = x->right;
    Node *T2 = y->left;

    // Perform rotation
    y->left = x;
    x->right = T2;

    // Update heights
    x->height = updateHeight(x);
    y->height = updateHeight(y);

    return y; // New root
}

// Balance the tree by performing rotations if needed
Node *AVLTree::balance(Node *node)
{
    node->height = updateHeight(node);
    int balance = balanceFactor(node);

    // Left-heavy
    if (balance > 1)
    {
        if (balanceFactor(node->left) < 0)
        {
            node->left = rotateLeft(node->left); // Left-Right case
        }
        return rotateRight(node); // Left-Left case
    }

    // Right-heavy
    if (balance < -1)
    {
        if (balanceFactor(node->right) > 0)
        {
            node->right = rotateRight(node->right); // Right-Left case
        }
        return rotateLeft(node); // Right-Right case
    }

    return node; // Already balanced
}

// Insert a key-value pair and balance the tree
Node *AVLTree::put(Node *node, int64_t key, int64_t value)
{
    if (!node)
        return new Node(key, value);

    if (key < node->key)
    {
        node->left = put(node->left, key, value);
    }
    else if (key > node->key)
    {
        node->right = put(node->right, key, value);
    }
    else
    {
        node->value = value; // Update the value if the key already exists
    }

    return balance(node);
}

// Public interface to insert a key-value pair
void AVLTree::put(int64_t key, int64_t value)
{
    current_size++;
    root = put(root, key, value);
}

// Public interface to delete a key
void AVLTree::del(int64_t key)
{
    // Insert a tombstone value to mark the key as deleted
    put(key, INT64_MIN);
}

int AVLTree::getCurrentSize() const
{
    return current_size;
}

// Search for a key and return its value
int64_t AVLTree::get(Node *node, int64_t key)
{
    if (!node)
        return -1; // Using -1 as a sentinel for "Key not found"

    if (key < node->key)
    {
        return get(node->left, key);
    }
    else if (key > node->key)
    {
        return get(node->right, key);
    }
    else
    {
        return node->value;
    }
}

// Public interface to search for a key
int64_t AVLTree::get(int64_t key)
{
    int64_t value = get(root, key);
    if (value == INT64_MIN)
    {
        return -1; // Return -1 if the key has been deleted
    }
    return value;
}

// Recursive in-order traversal to collect key-value pairs within the given range
void AVLTree::inOrderTraversal(Node *node, std::vector<std::pair<int64_t, int64_t>> &result, int64_t start, int64_t end)
{
    if (!node)
        return;

    if (node->key > start)
    {
        inOrderTraversal(node->left, result, start, end);
    }

    if (node->key >= start && node->key <= end && node->value != INT64_MIN)
    {
        result.push_back({node->key, node->value});
    }

    if (node->key < end)
    {
        inOrderTraversal(node->right, result, start, end);
    }
}

// Public interface for scanning the memtable within a range [start, end]
std::vector<std::pair<int64_t, int64_t>> AVLTree::scan(int64_t start, int64_t end)
{
    std::vector<std::pair<int64_t, int64_t>> memtableResult;
    inOrderTraversal(root, memtableResult, start, end);

    return memtableResult; // Only return results from the memtable
}

void AVLTree::clear()
{
    clearTree(root); // Helper function to recursively delete nodes
    root = nullptr;
    current_size = 0;
}

// Helper function to recursively delete nodes in the tree
void AVLTree::clearTree(Node *node)
{
    if (node)
    {
        clearTree(node->left);
        clearTree(node->right);
        delete node;
    }
}