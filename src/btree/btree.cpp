#include "btree.h"
#include <iostream>
#include <cmath>
#include "global/globals.h"

/**
 * @brief Constructor for Node.
 *
 * Initializes a node as either a leaf or internal node.
 *
 * @param isLeaf True if the node is a leaf node, false if it's an internal node.
 */
BTree::Node::Node(bool isLeaf) : isLeaf(isLeaf) {
    // Initialize vectors based on node type
    if (isLeaf) {
        // Leaf nodes will store keys and corresponding offsets
    } else {
        // Internal nodes will store keys and child pointers
    }
}

/**
 * @brief Destructor for Node.
 *
 * Recursively deletes child nodes if the node is not a leaf node.
 */
BTree::Node::~Node() {
    if (!isLeaf) {
        for (auto child : children) {
            delete child;
        }
    }
}

/**
 * @brief Constructor for BTree.
 *
 * Initializes the B-tree with the given minimum degree.
 *
 * @param degree The minimum degree of the B-tree (t).
 */
BTree::BTree(int degree) : root(nullptr), degree(degree) {}

/**
 * @brief Destructor for BTree.
 *
 * Deletes the root node, which recursively deletes all child nodes.
 */
BTree::~BTree() {
    if (root) {
        delete root;
    }
}

/**
 * @brief Inserts a key and offset into the B-tree.
 *
 * If the root is full, splits the root and creates a new root.
 * Otherwise, inserts the key into the appropriate subtree.
 *
 * @param key The key to insert.
 * @param offset The offset associated with the key.
 */
void BTree::insert(int64_t key, int64_t offset) {
    if (root == nullptr) {
        // If tree is empty, create a new leaf node and insert the key
        root = new Node(true);
        root->keys.push_back(key);
        root->offsets.push_back(offset);
        return;
    }

    if (root->keys.size() == 2 * degree - 1) {
        // If root is full, split it and create a new root
        Node* newRoot = new Node(false);
        newRoot->children.push_back(root);
        splitChild(newRoot, 0, root);

        // Update the root
        root = newRoot;
    }

    // Insert the key into the non-full node
    insertNonFull(root, key, offset);
}

/**
 * @brief Helper function to insert a key into a non-full node.
 *
 * @param node The node to insert the key into.
 * @param key The key to insert.
 * @param offset The offset associated with the key.
 */
void BTree::insertNonFull(Node* node, int64_t key, int64_t offset) {
    int i = node->keys.size() - 1;

    if (node->isLeaf) {
        // Insert the key into the leaf node in sorted order
        node->keys.push_back(0);      // Temporary value to expand the vector
        node->offsets.push_back(0);   // Temporary value

        while (i >= 0 && key < node->keys[i]) {
            node->keys[i + 1] = node->keys[i];
            node->offsets[i + 1] = node->offsets[i];
            i--;
        }

        node->keys[i + 1] = key;
        node->offsets[i + 1] = offset;
    } else {
        // Find the child which is going to have the new key
        while (i >= 0 && key < node->keys[i]) {
            i--;
        }
        i++;

        // Check if the found child is full
        if (node->children[i]->keys.size() == 2 * degree - 1) {
            // Split the child
            splitChild(node, i, node->children[i]);

            // After split, the middle key moves up and node->children[i] is split into two
            if (key > node->keys[i]) {
                i++;
            }
        }

        // Recurse on the appropriate child
        insertNonFull(node->children[i], key, offset);
    }
}

/**
 * @brief Helper function to split a full child node.
 *
 * @param parent The parent node of the child.
 * @param index The index of the child in the parent's children vector.
 * @param child The child node to split.
 */
void BTree::splitChild(Node* parent, int index, Node* child) {
    // Create a new node which will store (degree - 1) keys of child
    Node* newNode = new Node(child->isLeaf);
    for (int j = 0; j < degree - 1; j++) {
        newNode->keys.push_back(child->keys[j + degree]);
    }

    if (child->isLeaf) {
        // Copy the last (degree - 1) offsets to the new node
        for (int j = 0; j < degree - 1; j++) {
            newNode->offsets.push_back(child->offsets[j + degree]);
        }
        // Remove the moved keys and offsets from the original child
        child->keys.resize(degree);
        child->offsets.resize(degree);
    } else {
        // Copy the last degree children to the new node
        for (int j = 0; j < degree; j++) {
            newNode->children.push_back(child->children[j + degree]);
        }
        // Remove the moved keys and children from the original child
        child->keys.resize(degree - 1);
        child->children.resize(degree);
    }

    // Insert a new key into the parent node
    parent->keys.insert(parent->keys.begin() + index, child->keys[degree - 1]);

    // Insert the new child into the parent's children vector
    parent->children.insert(parent->children.begin() + index + 1, newNode);

}

/**
 * @brief Performs a pre-order traversal of the B-tree.
 *
 * Visits the root node first, then recursively visits each child from left to right.
 * Collects all nodes into a vector.
 *
 * @return A vector containing pointers to all nodes in pre-order.
 */
std::vector<BTree::Node*> BTree::preorderTraversal() {
    std::vector<Node*> nodes;
    preorderTraversalHelper(root, nodes);
    return nodes;
}

/**
 * @brief Helper function to perform pre-order traversal recursively.
 *
 * @param node The current node being visited.
 * @param nodes The vector to collect nodes.
 */
void BTree::preorderTraversalHelper(Node* node, std::vector<Node*>& nodes) {
    if (!node) return;

    // Visit the current node
    nodes.push_back(node);

    // Recursively visit each child
    if (!node->isLeaf) {
        for (auto child : node->children) {
            preorderTraversalHelper(child, nodes);
        }
    }
}

/**
 * @brief Performs a post-order traversal of the B-tree.
 *
 * Visits the children first, then the root node, recursively from left to right.
 * Collects all nodes into a vector.
 *
 * @return A vector containing pointers to all nodes in post-order.
 */
std::vector<BTree::Node*> BTree::postorderTraversal() {
    std::vector<Node*> nodes;
    postorderTraversalHelper(root, nodes);
    return nodes;
}

/**
 * @brief Helper function to perform post-order traversal recursively.
 *
 * Visits all children of the current node first and then the node itself.
 *
 * @param node The current node being visited.
 * @param nodes The vector to collect nodes in post-order.
 */
void BTree::postorderTraversalHelper(Node* node, std::vector<Node*>& nodes) {
    if (!node) return;

    // Recursively visit each child first
    if (!node->isLeaf) {
        for (auto child : node->children) {
            postorderTraversalHelper(child, nodes);
        }
    }

    // Visit the current node after its children
    nodes.push_back(node);
}


/**
 * @brief Prints the B-tree in a readable format.
 *
 * Performs a level-order traversal to print each level of the B-tree.
 */
void BTree::printTree() {
    if (!root) {
        std::cout << "The B-tree is empty." << std::endl;
        return;
    }

    std::vector<Node*> currentLevel;
    currentLevel.push_back(root);

    while (!currentLevel.empty()) {
        std::vector<Node*> nextLevel;

        // Iterate through all nodes in the current level
        for (auto node : currentLevel) {
            // Print all keys in the current node
            std::cout << "keys in current node[ ";
            for (size_t i = 0; i < node->keys.size(); ++i) {
                std::cout << node->keys[i];
                if (i != node->keys.size() - 1) {
                    std::cout << ", ";
                }
            }
            std::cout << " ] ";
            std::cout << "Data of the node: " << std::endl;
            // Print the data field of the current node
            // node->printData();

            // Collect all children for the next level
            if (!node->isLeaf) {
                for (auto child : node->children) {
                    nextLevel.push_back(child);
                }
            }
        }

        std::cout << std::endl;
        currentLevel = nextLevel;
    }
    std::cout << "The B-tree is above" << std::endl;
}

/**
 * @brief Updates the data field to reflect the current state of the node.
 *
 * This function packs the keys, offsets, and metadata into the data vector.
 */
void BTree::Node::updateData() {
    // Calculate the required size for the data vector
    size_t requiredSize = PAGE_SIZE;
    data.resize(requiredSize);
    size_t metadataOffset = 0;

    // Store the number of keys
    int32_t keyCount = static_cast<int32_t>(keys.size()); // Store the size in a variable
    std::memcpy(&data[metadataOffset], &keyCount, sizeof(keyCount));
    metadataOffset += sizeof(keyCount);
    int32_t offCount = static_cast<int32_t>(offsets.size()); // Store the size in a variable
    std::memcpy(&data[metadataOffset], &offCount, sizeof(offCount));
    metadataOffset += sizeof(offCount);
    // Store the keys and offsets
    for (size_t i = 0; i < keyCount; ++i) {
        std::memcpy(&data[metadataOffset], &offsets[i], sizeof(offsets[i]));
        metadataOffset += sizeof(offsets[i]);
        std::memcpy(&data[metadataOffset], &keys[i], sizeof(keys[i]));
        metadataOffset += sizeof(keys[i]);
    }

    // Store the last offsets if possible
    if (keyCount < offCount) {
        std::memcpy(&data[metadataOffset], &offsets[keyCount], sizeof(offsets[keyCount]));
        metadataOffset += sizeof(offsets[keyCount]);
    }


}

void BTree::Node::printData() {
    std::cout << "Data field (size = " << data.size() << "): ";
    for (size_t i = 0; i < data.size(); ++i) {
        // Print each byte in hexadecimal format for better readability
        printf("%02X ", static_cast<unsigned char>(data[i]));
    }
    std::cout << std::endl;
}

BTree::Node* BTree::getRoot() {
    return root;
}
