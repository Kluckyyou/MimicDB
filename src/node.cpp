#include <iostream>

struct Node
{
    int64_t key;
    int64_t value;
    Node *left;
    Node *right;
    int height;

    Node(int64_t k, int64_t v)
        : key(k), value(v), left(nullptr), right(nullptr), height(1) {}
};