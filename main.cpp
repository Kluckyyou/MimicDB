#include "kvstore.h"
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cctype>
#include <memory>

// Constants
const size_t DEFAULT_MEMTABLE_SIZE = 5; // default size if no size has been specified by user

// Utility function to split input
std::vector<std::string> splitInput(const std::string &input)
{
    std::stringstream ss(input);
    std::string token;
    std::vector<std::string> tokens;

    while (ss >> token)
    {
        tokens.push_back(token);
    }
    return tokens;
}

int main()
{
    std::unique_ptr<KVStore> kvStore;
    bool isOpen = false;
    std::string currentDB;

    while (true)
    {
        std::cout << "kvstore> ";
        std::string inputLine;
        std::getline(std::cin, inputLine);

        // Remove leading/trailing whitespace
        inputLine.erase(0, inputLine.find_first_not_of(" \t"));
        inputLine.erase(inputLine.find_last_not_of(" \t") + 1);

        // Check for empty input
        if (inputLine.empty())
        {
            continue;
        }

        // Split input into command and arguments
        std::vector<std::string> tokens = splitInput(inputLine);
        std::string command = tokens[0];

        // Convert command to lowercase for case-insensitive matching
        for (auto &c : command)
            c = std::tolower(c);

        if (command == "open")
        {
            if (tokens.size() < 2 || tokens.size() > 3)
            {
                std::cout << "Usage: open <db_name> [memtable_size]" << std::endl;
                continue;
            }
            if (isOpen)
            {
                kvStore->Close();
            }
            currentDB = tokens[1];
            size_t memtableSize = DEFAULT_MEMTABLE_SIZE;

            if (tokens.size() == 3)
            {
                // Directly parse the memtable size
                memtableSize = std::stoull(tokens[2]);
            }

            kvStore = std::make_unique<KVStore>(memtableSize);
            kvStore->Open(currentDB);
            isOpen = true;
            std::cout << "Database '" << currentDB << "' opened with memtable size " << memtableSize << "." << std::endl;
        }
        else if (command == "close")
        {
            if (!isOpen)
            {
                std::cout << "No database is currently open." << std::endl;
                continue;
            }
            kvStore->Close();
            kvStore.reset();
            isOpen = false;
            std::cout << "Database '" << currentDB << "' closed." << std::endl;
        }
        else if (command == "put")
        {
            if (tokens.size() != 3)
            {
                std::cout << "Usage: put <key> <value>" << std::endl;
                continue;
            }
            if (!isOpen)
            {
                std::cout << "No database is open. Use 'open <db_name> [memtable_size]' to open a database." << std::endl;
                continue;
            }
            int64_t key = std::stoll(tokens[1]);
            int64_t value = std::stoll(tokens[2]);
            kvStore->Put(key, value);
            std::cout << "Inserted key: " << key << ", value: " << value << std::endl;
        }
        else if (command == "get")
        {
            if (tokens.size() != 2)
            {
                std::cout << "Usage: get <key>" << std::endl;
                continue;
            }
            if (!isOpen)
            {
                std::cout << "No database is open. Use 'open <db_name> [memtable_size]' to open a database." << std::endl;
                continue;
            }
            int64_t key = std::stoll(tokens[1]);
            int64_t value = kvStore->Get(key);
            if (value != INT64_MIN)
            { // Assuming INT64_MIN indicates key not found
                std::cout << "Value for key " << key << ": " << value << std::endl;
            }
            else
            {
                std::cout << "Key " << key << " not found." << std::endl;
            }
        }
        else if (command == "del")
        {
            if (tokens.size() != 2)
            {
                std::cout << "Usage: del <key>" << std::endl;
                continue;
            }
            if (!isOpen)
            {
                std::cout << "No database is open. Use 'open <db_name> [memtable_size]' to open a database." << std::endl;
                continue;
            }
            int64_t key = std::stoll(tokens[1]);
            kvStore->Del(key);
            std::cout << "Deleted key: " << key << std::endl;
        }
        else if (command == "scan")
        {
            if (tokens.size() != 3)
            {
                std::cout << "Usage: scan <start_key> <end_key>" << std::endl;
                continue;
            }
            if (!isOpen)
            {
                std::cout << "No database is open. Use 'open <db_name> [memtable_size]' to open a database." << std::endl;
                continue;
            }
            int64_t startKey = std::stoll(tokens[1]);
            int64_t endKey = std::stoll(tokens[2]);
            if (startKey > endKey)
            {
                std::cout << "Error: start_key must be less than or equal to end_key." << std::endl;
                continue;
            }
            int resultCount = 0;
            std::pair<int64_t, int64_t> *results = kvStore->Scan(startKey, endKey, resultCount);
            if (resultCount > 0)
            {
                std::cout << "Scan results:" << std::endl;
                for (int i = 0; i < resultCount; ++i)
                {
                    std::cout << "Key: " << results[i].first << ", Value: " << results[i].second << std::endl;
                }
            }
            else
            {
                std::cout << "No keys found in the specified range." << std::endl;
            }
            delete[] results;
        }
        else if (command == "usebtree")
        {
            if (!isOpen)
            {
                std::cout << "No database is open. Use 'open <db_name> [memtable_size]' to open a database." << std::endl;
                continue;
            }

            if (tokens.size() != 2)
            {
                std::cout << "Usage: usebtree <flag>" << std::endl;
                continue;
            }

            bool flag;
            std::string flagStr = tokens[1];

            // Convert input to boolean, accepting "true", "false", "1", or "0"
            if (flagStr == "true" || flagStr == "1")
            {
                flag = true;
            }
            else if (flagStr == "false" || flagStr == "0")
            {
                flag = false;
            }
            else
            {
                std::cout << "Invalid flag. Use 'true', 'false', '1', or '0'." << std::endl;
                continue;
            }

            kvStore->SetUseBTree(flag);
            std::cout << "Using BTree set to: " << (flag ? "true" : "false") << std::endl;
        }
        else if (command == "help")
        {
            std::cout << "Available commands:" << std::endl;
            std::cout << "  open <db_name> [memtable_size]            Open a database with optional memtable size" << std::endl;
            std::cout << "  close                                     Close the current database" << std::endl;
            std::cout << "  put <key> <value>                         Insert or update a key-value pair" << std::endl;
            std::cout << "  get <key>                                 Retrieve the value for a key" << std::endl;
            std::cout << "  del <key>                                 Delete a key-value pair" << std::endl;
            std::cout << "  scan <start_key> <end_key>                Retrieve key-value pairs in a key range" << std::endl;
            std::cout << "  usebtree <flag>                           Use Btree search or not" << std::endl;
            std::cout << "  exit, quit                                Exit the program" << std::endl;
        }
        else if (command == "exit" || command == "quit")
        {
            if (isOpen)
            {
                kvStore->Close();
                std::cout << "Database '" << currentDB << "' closed." << std::endl;
            }
            std::cout << "Exiting KVStore." << std::endl;
            break;
        }
        else
        {
            std::cout << "Unknown command: " << command << std::endl;
            std::cout << "Type 'help' to see available commands." << std::endl;
        }
    }

    return 0;
}
