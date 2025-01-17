// HashMap.h
#ifndef HASHMAP_H
#define HASHMAP_H

#include <vector>
#include <functional>
#include "murmur3.h"

// Define HashNode structure
template <typename K, typename V>
struct HashNode {
    K key;
    V value;
    HashNode* next;

    HashNode(const K& key, const V& value) : key(key), value(value), next(nullptr) {}
};

// Define HashMap class
template <typename K, typename V>
class HashMap {
private:
    std::vector<HashNode<K, V>*> table;
    size_t capacity;
    size_t size;

    size_t hashFunction(const K& key) const {
        uint32_t hash;
        MurmurHash3_x86_32(reinterpret_cast<const char*>(key.c_str()), key.size(), 0, &hash);
        return hash % capacity;
    }

public:
    HashMap(size_t capacity = 2560);
    void insert(const K& key, const V& value);
    V* get(const K& key);
    void remove(const K& key);
    ~HashMap();
};

// Include implementation details for templates
#include "HashMap.tpp"

#endif  // HASHMAP_H
