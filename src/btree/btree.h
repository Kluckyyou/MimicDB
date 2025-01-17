#ifndef BTREE_H
#define BTREE_H

#include <vector>
#include <cstdint>
#include <cstring>
#include <iostream>

/**
 * @brief BTree class representing a static B-tree for searching in SSTs.
 *
 * The BTree class provides functionality to build a B-tree from keys and page offsets,
 * insert new keys, search for keys, and serialize/deserialize the tree.
 * This implementation uses a fixed degree (t = 128) to satisfy the constraints:
 * - Maximum number of keys in a node: 2t - 1 = 255
 * - Maximum number of children in a node: 2t = 256
 */
class BTree {
public:
    /**
     * @brief Nested Node class representing a node in the B-tree.
     */
    class Node {
    public:
        bool isLeaf;                          // True if node is a leaf node
        std::vector<int64_t> keys;            // Keys stored in this node
        std::vector<Node*> children;          // Child nodes (for internal nodes)
        std::vector<int64_t> offsets;         // Offsets to pages (only for leaf nodes), Offset to child nodes written in SST (for internal nodes)
        std::vector<char> data;               // Raw data representing the Node contents

        /**
         * @brief Constructor for Node.
         *
         * @param isLeaf Indicates whether the node is a leaf node.
         */
        Node(bool isLeaf);

        /**
         * @brief Destructor for Node.
         *
         * Recursively deletes child nodes if the node is not a leaf.
         */
        ~Node();

        void updateData();  // Function to update the data field
        void printData();
    };

    /**
     * @brief Constructor for BTree.
     *
     * @param degree The minimum degree of the B-tree (t). Determines the range of keys per node.
     */
    BTree(int degree);

    /**
     * @brief Destructor for BTree.
     *
     * Deletes the root node, which recursively deletes all child nodes.
     */
    ~BTree();

    /**
     * @brief Inserts a key and offset into the B-tree.
     *
     * If the root is full, splits the root and creates a new root.
     * Otherwise, inserts the key into the appropriate subtree.
     *
     * @param key The key to insert.
     * @param offset The offset associated with the key.
     */
    void insert(int64_t key, int64_t offset);

    /**
     * @brief Performs a pre-order traversal of the B-tree.
     *
     * Visits the root node first, then recursively visits each child from left to right.
     * Collects all nodes into a vector.
     *
     * @return A vector containing pointers to all nodes in pre-order.
     */
    std::vector<Node*> preorderTraversal();
    std::vector<Node*> postorderTraversal();
    void printTree();  // Function to print the B-tree
    Node* getRoot();

private:
    Node* root;       // Root node of the B-tree
    int degree;       // Minimum degree of the B-tree (t)

    /**
     * @brief Helper function to insert a key into a non-full node.
     *
     * @param node The node to insert the key into.
     * @param key The key to insert.
     * @param offset The offset associated with the key.
     */
    void insertNonFull(Node* node, int64_t key, int64_t offset);

    /**
     * @brief Helper function to split a full child node.
     *
     * @param parent The parent node of the child.
     * @param index The index of the child in the parent's children vector.
     * @param child The child node to split.
     */
    void splitChild(Node* parent, int index, Node* child);

    /**
     * @brief Helper function to perform pre-order traversal recursively.
     *
     * @param node The current node being visited.
     * @param nodes The vector to collect nodes.
     */
    void preorderTraversalHelper(Node* node, std::vector<Node*>& nodes);
    void postorderTraversalHelper(Node* node, std::vector<Node*>& nodes);

};

#endif // BTREE_H

