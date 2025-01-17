#include <iostream>
#include <string>
#include <assert.h>
#include "../page/page.h"
#include "../sst/sst.h"
#include "../memtable/memtable.h"
#include "../btree/btree.h"
#include "../bloomfilter/bloomfilter.h"
#include "../bufferpool/HashMap.h"
#include "../bufferpool/bufferpool.h"
#include "../kvstore.h"

int runTest(const std::string &testName, bool (*testFunction)())
{
    std::cout << "Running test: " << testName << "... ";
    if (testFunction())
    {
        std::cout << "PASSED" << std::endl;
        return 0;
    }
    else
    {
        std::cout << "FAILED" << std::endl;
        return 1;
    }
}

bool testPageAddEntry()
{
    Page page;
    bool result = page.addEntry(1, 42); // Example key-value pair
    return result && page.numEntries == 1;
}

bool testSSTMetadata()
{
    SST sst;
    Page page;
    page.addEntry(1, 42);
    page.addEntry(2, 84);
    sst.addPage(page);

    return (sst.numEntries == 2 && sst.numPages == 1);
}

bool testAVLTreeInitialization()
{
    AVLTree tree(10);                  // Initialize with a max size of 10
    return tree.getCurrentSize() == 0; // Ensure the size is initially 0
}

bool testAVLPutAndGet()
{
    AVLTree tree(10);
    tree.put(1, 42); // Insert key-value pair
    tree.put(2, 84); // Insert another pair

    return (tree.get(1) == 42) && (tree.get(2) == 84);
}

bool testAVLDelete()
{
    AVLTree tree(10);
    tree.put(1, 42); // Insert key-value pair
    tree.del(1);     // Delete the key

    return (tree.get(1) == -1);
}

bool testAVLScan()
{
    AVLTree tree(10);
    tree.put(1, 42);
    tree.put(2, 84);
    tree.put(3, 126);

    // Scan for all entries in range [1, 3]
    auto result = tree.scan(1, 3);

    return result.size() == 3;
}

bool testHashMapInitialization()
{
    HashMap<std::string, int> hashMap(100); // Initialize with a capacity of 100
    return true;                            // If the object is constructed successfully, the test passes
}

bool testHashMapInsertAndGet()
{
    HashMap<std::string, int> hashMap;
    hashMap.insert("key1", 42);
    hashMap.insert("key2", 84);

    int *value1 = hashMap.get("key1");
    int *value2 = hashMap.get("key2");

    return (value1 && *value1 == 42) && (value2 && *value2 == 84);
}

bool testHashMapRemove()
{
    HashMap<std::string, int> hashMap;
    hashMap.insert("key1", 42);
    hashMap.remove("key1");

    int *value = hashMap.get("key1"); // Should return nullptr after removal
    return value == nullptr;
}

bool testHashMapCollisionHandling()
{
    HashMap<std::string, int> hashMap(1); // Force collisions by setting capacity to 1

    hashMap.insert("key1", 42);
    hashMap.insert("key2", 84);

    int *value1 = hashMap.get("key1");
    int *value2 = hashMap.get("key2");

    return (value1 && *value1 == 42) && (value2 && *value2 == 84);
}

bool testHashMapKeyNotFound()
{
    HashMap<std::string, int> hashMap;

    hashMap.insert("key1", 42);
    int *value = hashMap.get("key2"); // Key doesn't exist

    return value == nullptr; // Should return nullptr for missing keys
}

bool testBufferPoolInitialization()
{
    BufferPool bufferPool(4); // Initialize with a capacity of 4
    return true;              // If the object is constructed successfully, the test passes
}

bool testBufferPoolInsertAndGetPage()
{
    BufferPool bufferPool(4);
    Page page1, page2;

    bufferPool.insertPage("page1", page1);
    bufferPool.insertPage("page2", page2);

    Page *retrievedPage1 = bufferPool.getPage("page1");
    Page *retrievedPage2 = bufferPool.getPage("page2");

    return (retrievedPage1 != nullptr) && (retrievedPage2 != nullptr);
}

bool testBufferPoolEviction()
{
    BufferPool bufferPool(2); // Capacity set to 2
    Page page1, page2, page3;

    bufferPool.insertPage("page1", page1);
    bufferPool.insertPage("page2", page2);

    // Access page1 to set its reference bit
    bufferPool.getPage("page1");

    // Insert a new page, triggering eviction
    bufferPool.insertPage("page3", page3);

    // Ensure page2 (least recently accessed) is evicted
    Page *retrievedPage2 = bufferPool.getPage("page2");
    Page *retrievedPage3 = bufferPool.getPage("page3");

    return (retrievedPage2 == nullptr) && (retrievedPage3 != nullptr);
}

bool testBufferPoolMultipleEvictions()
{
    BufferPool bufferPool(3); // Capacity set to 3
    Page page1, page2, page3, page4, page5;

    bufferPool.insertPage("page1", page1);
    bufferPool.insertPage("page2", page2);
    bufferPool.insertPage("page3", page3);

    // Access page1 to set its reference bit
    bufferPool.getPage("page1");

    // Insert two new pages, triggering multiple evictions
    bufferPool.insertPage("page4", page4);
    bufferPool.insertPage("page5", page5);

    Page *retrievedPage1 = bufferPool.getPage("page1");
    Page *retrievedPage2 = bufferPool.getPage("page2"); // Should be evicted
    Page *retrievedPage3 = bufferPool.getPage("page3"); // Should be evicted

    return (retrievedPage1 != nullptr) &&
           (retrievedPage2 == nullptr) &&
           (retrievedPage3 == nullptr);
}

bool testBufferPoolPageReplacement()
{
    BufferPool bufferPool(2); // Capacity set to 2
    Page page1, page2, page3;

    bufferPool.insertPage("page1", page1);
    bufferPool.insertPage("page2", page2);

    // Insert a new page, replacing the least recently used page
    bufferPool.insertPage("page3", page3);

    Page *retrievedPage1 = bufferPool.getPage("page1"); // Should be evicted
    Page *retrievedPage3 = bufferPool.getPage("page3");

    return (retrievedPage1 == nullptr) && (retrievedPage3 != nullptr);
}

/**
 * @brief Test the insertion of keys into the B-tree.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBtreeInsert()
{
    BTree tree(2); // Degree 2 B-tree
    tree.insert(5, 50);
    tree.insert(6, 60);
    tree.insert(10, 100);
    tree.insert(12, 120);
    tree.insert(20, 200);

    auto root = tree.getRoot();
    // std::cout << "PASSED1" << std::endl;
    if (!root || root->keys.size() != 1 || root->keys[0] != 6)
        return false;

    auto leftChild = root->children[0];
    auto rightChild = root->children[1];
    // std::cout << "PASSED2" << std::endl;
    // std::cout << leftChild->keys[1] << std::endl;
    if (leftChild->keys.size() != 2 || leftChild->keys[0] != 5 || leftChild->keys[1] != 6)
        return false;
    // std::cout << rightChild->keys[0] << std::endl;
    // std::cout << rightChild->keys[1] << std::endl;
    // std::cout << rightChild->keys[2] << std::endl;
    if (rightChild->keys.size() != 3 || rightChild->keys[0] != 10 || rightChild->keys[1] != 12 || rightChild->keys[2] != 20)
        return false;

    return true;
}

/**
 * @brief Test pre-order traversal of the B-tree.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBtreePreorderTraversal()
{
    BTree tree(2);
    tree.insert(10, 100);
    tree.insert(20, 200);
    tree.insert(5, 50);
    tree.insert(6, 60);
    tree.insert(12, 120);

    auto nodes = tree.preorderTraversal();
    if (nodes.empty())
        return false;

    if (nodes[0]->keys[0] != 10)
        return false;
    if (nodes[1]->keys[0] != 5 || nodes[1]->keys[1] != 6)
        return false;
    if (nodes[2]->keys[0] != 12 || nodes[2]->keys[1] != 20)
        return false;

    return true;
}

/**
 * @brief Test post-order traversal of the B-tree.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBtreePostorderTraversal()
{
    BTree tree(2);
    tree.insert(10, 100);
    tree.insert(20, 200);
    tree.insert(5, 50);
    tree.insert(6, 60);
    tree.insert(12, 120);

    auto nodes = tree.postorderTraversal();
    if (nodes.empty())
        return false;

    // The last node in post-order traversal should be the root
    auto root = tree.getRoot();
    if (nodes.back() != root)
        return false;

    return true;
}

/**
 * @brief Test splitting nodes in the B-tree.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBtreeNodeSplitting()
{
    BTree tree(2);
    tree.insert(10, 100);
    tree.insert(20, 200);
    tree.insert(30, 300);
    tree.insert(40, 400);

    auto root = tree.getRoot();
    if (!root || root->keys.size() != 1 || root->keys[0] != 20)
        return false;

    auto leftChild = root->children[0];
    auto rightChild = root->children[1];

    if (leftChild->keys.size() != 2 || leftChild->keys[0] != 10 || leftChild->keys[1] != 20)
        return false;
    if (rightChild->keys.size() != 2 || rightChild->keys[0] != 30 || rightChild->keys[1] != 40)
        return false;

    return true;
}

/**
 * @brief Test the updateData function for nodes.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBtreeUpdateData()
{
    BTree::Node node(true);
    node.keys = {10, 20, 30};
    node.offsets = {100, 200, 300, 400}; // 4 offsets, one more than keys
    node.updateData();

    if (node.data.empty())
        return false;

    int32_t keyCount, offsetCount;
    std::memcpy(&keyCount, &node.data[0], sizeof(keyCount));
    std::memcpy(&offsetCount, &node.data[sizeof(keyCount)], sizeof(offsetCount));

    if (keyCount != 3 || offsetCount != 4)
        return false;

    return true;
}

/**
 * @brief Test the insertion and querying functionality of the Bloom filter.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBloomfilterInsertAndQuery()
{
    BloomFilter filter(100, 10);

    // Insert keys into the Bloom filter
    filter.insert(12345);
    filter.insert(67890);

    // Verify the keys can be queried
    if (!filter.query(12345))
        return false;
    if (!filter.query(67890))
        return false;

    // Verify a non-inserted key is not in the filter
    if (filter.query(11111))
        return false;

    return true;
}

/**
 * @brief Test the false positive rate of the Bloom filter.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBloomfilterFalsePositives()
{
    BloomFilter filter(100, 10);

    // Insert keys into the Bloom filter
    filter.insert(12345);
    filter.insert(67890);

    // Test for false positives (should not always fail but be very low)
    int falsePositives = 0;
    for (int i = 0; i < 1000; ++i)
    {
        if (i != 12345 && i != 67890 && filter.query(i))
        {
            ++falsePositives;
        }
    }

    // Allow up to a small percentage of false positives
    return falsePositives < 10; // Adjust threshold based on configuration
}

/**
 * @brief Test the updateData functionality of the Bloom filter.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBloomfilterUpdateData()
{
    BloomFilter filter(100, 10);

    // Insert keys into the Bloom filter
    filter.insert(12345);
    filter.insert(67890);

    // Update the data vector
    filter.updateData();

    // Ensure the data vector is not empty
    if (filter.data.empty())
        return false;

    // Ensure the data size matches the expected byte size
    if (filter.data.size() != PAGE_SIZE)
        return false;

    return true;
}

/**
 * @brief Test the hash function of the Bloom filter.
 *
 * @return true if the test passes, false otherwise.
 */
bool testBloomfilterHashFunction()
{
    BloomFilter filter(100, 10);

    // Generate hash values for a key
    std::vector<int> hashes = filter.getHashValues(12345);

    // Ensure the correct number of hash values is generated
    if ((int)hashes.size() != (int)filter.numHashFunctions)
        return false;

    // Ensure the hash values are within bounds
    for (int hash : hashes)
    {
        if (hash < 0 || hash >= filter.numBits)
            return false;
    }

    return true;
}

// testing kvstore API
bool testKVStore()
{

    // Initialize with small memtable size for basic testing
    KVStore kvStore(3);

    // Open the database
    kvStore.Open("test_db");

    // Insert some key value pairs for basic testing
    kvStore.Put(10, 10010);
    kvStore.Put(20, 10020);
    kvStore.Put(25, 10025); // Should flush -> sst_1

    kvStore.Put(30, 10030);
    kvStore.Put(10, 10011); // Should update the value of key 10
    kvStore.Put(15, 10015); // Should flush -> sst_2

    kvStore.Del(25);        // Should delete key 25 by marking it as tombstone
    kvStore.Put(30, 10031); // Should update the value of key 30
    kvStore.Put(12, 10012); // Should flush -> sst_3

    kvStore.Put(100, 10100); // This remains in the memtable

    // Retrieve values using Get to ensure they are correctly stored
    assert(kvStore.Get(10) == 10011);
    assert(kvStore.Get(12) == 10012);
    assert(kvStore.Get(20) == 10020);
    assert(kvStore.Get(15) == 10015);
    assert(kvStore.Get(25) == -1); // Key was deleted and should not be found
    assert(kvStore.Get(30) == 10031);
    assert(kvStore.Get(100) == 10100); // have not been flushed but present in memtable
    assert(kvStore.Get(200) == -1);    // Key not found becuase it was never inserted

    // Retrieve values using Scan
    int result_count = 0;
    std::pair<int64_t, int64_t> *results;

    // Test Range [10, 20]
    results = kvStore.Scan(10, 20, result_count);
    assert(result_count == 4);
    assert(results[0].first == 10 && results[0].second == 10011);
    assert(results[1].first == 12 && results[1].second == 10012);
    assert(results[2].first == 15 && results[2].second == 10015);
    assert(results[3].first == 20 && results[3].second == 10020);
    delete[] results;

    // Test Range [15, 35]  (25 shoudl be deleted and therefore not present)
    results = kvStore.Scan(15, 35, result_count);
    assert(result_count == 3);
    assert(results[0].first == 15 && results[0].second == 10015);
    assert(results[1].first == 20 && results[1].second == 10020);
    assert(results[2].first == 30 && results[2].second == 10031);
    delete[] results;

    // Test Range [5, 100] (should include all keys in SSTs and memtable)
    results = kvStore.Scan(5, 100, result_count);
    assert(result_count == 6);
    assert(results[0].first == 10 && results[0].second == 10011);
    assert(results[1].first == 12 && results[1].second == 10012);
    assert(results[2].first == 15 && results[2].second == 10015);
    assert(results[3].first == 20 && results[3].second == 10020);
    assert(results[4].first == 30 && results[4].second == 10031);
    assert(results[5].first == 100 && results[5].second == 10100);
    delete[] results;

    // Test Range [200, 300] (should not include any key)
    results = kvStore.Scan(200, 300, result_count);
    assert(result_count == 0);
    delete[] results;

    kvStore.Close();

    return true; // all tests passed
}

// Main function to run all tests
int main()
{
    int failedTests = 0;

    // Entity tests
    failedTests += runTest("Page Add Entry", testPageAddEntry);
    failedTests += runTest("SST Metadata", testSSTMetadata);

    // AVLtree tests
    failedTests += runTest("AVLTree Initialization", testAVLTreeInitialization);
    failedTests += runTest("AVLTree Put and Get Operations", testAVLPutAndGet);
    failedTests += runTest("AVLTree Delete Operation", testAVLDelete);
    failedTests += runTest("AVLTree Scan Operation", testAVLScan);

    // Hashmap tests
    failedTests += runTest("Hashmap Initialization", testHashMapInitialization);
    failedTests += runTest("Hashmap Insert and Get", testHashMapInsertAndGet);
    failedTests += runTest("Hashmap Remove Operation", testHashMapRemove);
    failedTests += runTest("Hashmap Collision Handling", testHashMapCollisionHandling);
    failedTests += runTest("Hashmap Key Not Found", testHashMapKeyNotFound);

    // Buffer pool tests
    failedTests += runTest("Buffer Pool Initialization", testBufferPoolInitialization);
    failedTests += runTest("Buffer Pool Insert and Get Page", testBufferPoolInsertAndGetPage);
    failedTests += runTest("Buffer Pool Eviction Policy (Clock)", testBufferPoolEviction);
    failedTests += runTest("Buffer Pool Multiple Evictions", testBufferPoolMultipleEvictions);
    failedTests += runTest("Buffer Pool Page Replacement", testBufferPoolPageReplacement);

    // Btree tests
    failedTests += runTest("Btree Insertion", testBtreeInsert);
    failedTests += runTest("Btree Preorder Traversal", testBtreePreorderTraversal);
    failedTests += runTest("Btree Postorder Traversal", testBtreePostorderTraversal);
    failedTests += runTest("Btree Node Splitting", testBtreeNodeSplitting);
    failedTests += runTest("Btree Update Data", testBtreeUpdateData);

    // Bloom filter tests
    failedTests += runTest("Bloomfilter Insert and Query", testBloomfilterInsertAndQuery);
    failedTests += runTest("Bloomfilter False positive rates", testBloomfilterFalsePositives);
    failedTests += runTest("Bloomfilter Update Data", testBloomfilterUpdateData);
    failedTests += runTest("Bloomfilter Hash Value In Range", testBloomfilterHashFunction);

    // KVStore tests (user facing API)
    failedTests += runTest("KVStore API Tests with Debugging Messages", testKVStore);

    std::cout << "\nSummary: " << failedTests << " test(s) failed." << std::endl;
    return failedTests;
}