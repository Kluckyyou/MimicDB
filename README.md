# MimicDB: A Scalable Key-Value Store
Report can be found in CSC443_FINAL_PROJECT_REPORT.pdf.  
Code directory is ./src.  
Build directory is ./build

## Overview
MimicDB is a lightweight, scalable key-value store built from scratch as part of the CSC443 Database System Technology course. It implements an **LSM-tree architecture** with in-memory filters and is inspired by modern database technologies like RocksDB and Cassandra. The project demonstrates efficient data handling, query optimization, and robust API design for large-scale key-value storage systems.

## Features
- **Memtable**: In-memory AVL tree for efficient data insertion and querying.
- **SSTs (Sorted String Tables)**: Persistent storage with support for binary search.
- **Buffer Pool**: Optimized caching mechanism using a hash map and clock-based eviction policy.
- **Static B-Tree Indexing**: Enhanced query efficiency by structuring SSTs with B-Trees.
- **Bloom Filters**: Integrated to reduce unnecessary disk reads during key lookups.
- **LSM Tree Architecture**: Supports out-of-place updates, deletes with tombstones, and SST compaction.
- **Extensive Testing**: Unit tests for each module, ensuring reliability and correctness.

---

## API Usage

### Commands
1. **Open**
   ```
   open <db_name> [memtable_size]
   ```
   Opens or initializes a database. Optionally specify the memtable size.

2. **Close**
   ```
   close
   ```
   Closes the current database, flushing the memtable to SSTs.

3. **Put**
   ```
   put <key> <value>
   ```
   Inserts or updates a key-value pair in the database.

4. **Get**
   ```
   get <key>
   ```
   Retrieves the value associated with the specified key.

5. **Scan**
   ```
   scan <start_key> <end_key>
   ```
   Returns all key-value pairs in the specified range.

6. **Delete**
   ```
   del <key>
   ```
   Deletes the specified key by inserting a tombstone marker.

7. **Use B-Tree Search**
   ```
   usebtree <flag>
   ```
   Toggles between binary search and B-Tree-based search for SSTs.

8. **Exit**
   ```
   exit | quit
   ```
   Exits the MimicDB interface.

---

## Architecture
### 1. **Memtable**
- **Structure**: Implemented using an AVL tree for balanced in-memory storage.
- **Threshold**: Flushed to disk as an SST when memory capacity is reached.
- **Location**: Defined in `memtable.cpp` and `memtable.h`.

### 2. **SSTs**
- **Page Design**: 4KB pages with metadata, key-offset vector, and data sections.
- **Binary Search**: Supports efficient queries over persisted data.
- **File Management**: Metadata-first format for streamlined access.

### 3. **Buffer Pool**
- **Structure**: Hash map backed by MurmurHash with chaining for collision resolution.
- **Eviction**: Clock-based policy for efficient memory management.
- **Singleton**: Ensures global consistency and shared state.
- **Location**: Implemented in `bufferpool.cpp` and `HashMap.tpp`.

### 4. **Static B-Tree Indexing**
- **Design**: Layers of internal and leaf nodes persisted in postorder.
- **Root Access**: Positioned at the end of SST files for quick retrieval.
- **Dynamic Selection**: Choose between binary search and B-Tree search at runtime.
- **Location**: B-Tree logic resides in `btree.cpp`.

### 5. **LSM Tree with Bloom Filters**
- **Compaction**: Recursive merging of SSTs at larger levels.
- **Updates/Deletes**: Handles tombstones and ensures the latest key versions.
- **Bloom Filters**: Speeds up `Get` operations by pruning unnecessary file access.
- **Location**: Code for LSM Tree and filters is in `kvstore.cpp`.

---

## Performance
### Experiments
1. **Put Throughput**
   - Average: 1700 operations/sec with no performance degradation as data scales.

2. **Binary Search vs. B-Tree Index**
   - Binary Search: ~500 ops/sec.
   - B-Tree Search: ~800 ops/sec (~60% improvement).

3. **Scan Throughput**
   - Average: 1200 operations/sec due to sequential access and buffer pool utilization.

---

## Testing
- **Unit Tests**: Located in `unit_tests.cpp`. Categories include:
  1. Page and SST.
  2. AVL Tree.
  3. Hash Map and Buffer Pool.
  4. Static B-Tree.
  5. Bloom Filters.
  6. KVStore API.

---

## Compilation & Setup
1. **Environment**
   - **C++ Standard**: Requires g++ with `-std=c++17`.
   - Tested on:
     - Ubuntu 11.4.0 (with WSL or VM).
     - Windows 11.

2. **Build Instructions**
   ```bash
   cd build
   make
   ./main       # Start MimicDB CLI
   ./tests      # Run unit tests
   ```

3. **Files**
   - **Main Executable**: `main`
   - **Tests Executable**: `tests`

---

## Contributors
- **Andy Feng**: [andy.feng@mail.utoronto.ca](mailto:andy.feng@mail.utoronto.ca)
- **Kevin You**: [kevin.you@mail.utoronto.ca](mailto:kevin.you@mail.utoronto.ca)
- **Wilson Zhang**: [wilso.zhang@mail.utoronto.ca](mailto:wilso.zhang@mail.utoronto.ca)

---

## License
This project is for educational purposes and is not intended for commercial use. Contact contributors for inquiries.
