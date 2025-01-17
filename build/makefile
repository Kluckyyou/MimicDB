# Set the compiler and compilation flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2 -w -I../src -I../src/page -I../src/sst -I../src/memtable -I../src/global -I../src/bufferpool -I../src/btree -I../src/lsmtree -I../src/bloomfilter

# Define source directories and output
SRC_DIR = ../src
PAGE_DIR = $(SRC_DIR)/page
SST_DIR = $(SRC_DIR)/sst
MEMTABLE_DIR = $(SRC_DIR)/memtable
GLOBAL_DIR = $(SRC_DIR)/globals
BUFFER_DIR = $(SRC_DIR)/bufferpool
BTREE_DIR = $(SRC_DIR)/btree
LSMTREE_DIR = $(SRC_DIR)/lsmtree
BLOOMFILTER_DIR = $(SRC_DIR)/bloomfilter
TEST_DIR = $(SRC_DIR)/test
EXPERIMENTS_DIR = $(SRC_DIR)/experiments
OBJ_DIR = ../build
BIN = ../main

### TESTING ###
# Executable name for unit tests
TEST_EXEC = ../tests
EXPERIMENTS_EXEC = ../experiments

# Source files for the main program and tests
MAIN_SRC := ../main.cpp
TEST_SRC := $(TEST_DIR)/unit_tests.cpp
EXPERIMENTS_SRC := $(EXPERIMENTS_DIR)/experiments.cpp

# Source files for the main program and library files
SRCS := $(wildcard $(PAGE_DIR)/*.cpp) \
        $(wildcard $(SST_DIR)/*.cpp) \
        $(wildcard $(MEMTABLE_DIR)/*.cpp) \
        $(wildcard $(BTREE_DIR)/*.cpp) \
        $(wildcard $(BUFFER_DIR)/*.cpp) \
        $(wildcard $(SRC_DIR)/*.cpp) \
        $(wildcard $(LSMTREE_DIR)/*.cpp) \
        $(wildcard $(BLOOMFILTER_DIR)/*.cpp)

# Object files for the library (excluding main.cpp)
LIB_OBJS := $(SRCS:.cpp=.o)

# Object files for the main program
MAIN_OBJS := $(LIB_OBJS) $(MAIN_SRC:.cpp=.o)

# Object files for the unit tests (excluding main.cpp)
TEST_OBJS := $(LIB_OBJS) $(TEST_SRC:.cpp=.o)

# Object files for the experiments (excluding main.cpp)
EXPERIMENTS_OBJS := $(LIB_OBJS) $(EXPERIMENTS_SRC:.cpp=.o)

# Default target
all: $(BIN) $(TEST_EXEC) $(EXPERIMENTS_EXEC)

# Rule to build the main executable
$(BIN): $(MAIN_OBJS)
	$(CXX) $(CXXFLAGS) -o $(BIN) $(MAIN_OBJS)

# Rule to build the test executable
$(TEST_EXEC): $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) -o $(TEST_EXEC) $(TEST_OBJS)

# Rule to build the test executable
$(EXPERIMENTS_EXEC): $(EXPERIMENTS_OBJS)
	$(CXX) $(CXXFLAGS) -o $(EXPERIMENTS_EXEC) $(EXPERIMENTS_OBJS)

# Rule to compile each .cpp file into a .o file
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean target to remove all build artifacts
clean:
	rm -f $(LIB_OBJS) $(MAIN_OBJS) $(TEST_OBJS) $(BIN) $(TEST_EXEC) $(EXPERIMENTS_OBJS) $(EXPERIMENTS_EXEC)
