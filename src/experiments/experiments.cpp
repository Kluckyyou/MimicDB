#include "kvstore.h"
#include <iostream>
#include <fstream>
#include <cstdint>
#include <vector>
#include <cstring>
#include "page.h"
#include "bufferpool.h"
#include <chrono>
#include <random>
#include <limits>

int main()
{
    // There are 65536 16-byte units in 1 MB
    // Initialize KVStore with size 65536 to force flushing to SSTs
    KVStore kvStore(65536);

    // Create a random device to seed the generator
    std::random_device rd;

    // Initialize a 64-bit Mersenne Twister random number generator
    std::mt19937_64 gen(rd());

    // Calculate the maximum value (1677722 * 40)
    int64_t min_value = 1;
    int64_t max_value = 1677722LL * 40; // LL suffix ensures 64-bit literal

    // Define a uniform distribution over the entire range of int64_t
    std::uniform_int_distribution<int64_t> dist(min_value, max_value);

    // Open the database
    kvStore.Open("experiments_db");

    // Open a file to write the average total times
    std::ofstream output_file("average_times.txt");
    if (!output_file.is_open()) {
        std::cerr << "Failed to open the file for writing average times.\n";
        return 1;
    }

    for (int i = 0; i < 40; i++)
    {
        double sum_total_time_put = 0;
        for (int j = 0; j < 1677722; j++) 
        {
            // Generate a random int64_t key
            int64_t random_key = dist(gen);
            // start time
            auto start_time_put = std::chrono::system_clock::now();

            kvStore.Put(random_key, j);

            // end time
            auto end_time_put = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed_seconds_put = end_time_put - start_time_put;
            double total_time_put = elapsed_seconds_put.count();
            sum_total_time_put += total_time_put;
        }
        double avg_total_time_put = 1677722 / sum_total_time_put;

        double sum_total_time_binary = 0;
        double sum_total_time_btree = 0;
        for (int k = 0; k < 10; k++)
        {
            // Generate a random int64_t key
            int64_t random_key = dist(gen);

            kvStore.SetUseBTree(false);
            // start time
            auto start_time_binary = std::chrono::system_clock::now();
            kvStore.Get(random_key);
            // end time
            auto end_time_binary = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed_seconds_binary = end_time_binary - start_time_binary;
            double total_time_binary = elapsed_seconds_binary.count();
            sum_total_time_binary += total_time_binary;

            kvStore.SetUseBTree(true);
            // start time
            auto start_time_btree = std::chrono::system_clock::now();
            kvStore.Get(random_key);
            // end time
            auto end_time_btree = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed_seconds_btree = end_time_btree - start_time_btree;
            double total_time_btree = elapsed_seconds_btree.count();
            sum_total_time_btree += total_time_btree;
        }
        double avg_total_time_binary = 10 / sum_total_time_binary;
        double avg_total_time_btree = 10 / sum_total_time_btree;

        double sum_total_time_scan = 0;
        for (int l = 0; l < 10; l++) 
        {
            // Generate a random int64_t key
            int64_t random_start = dist(gen);
            int64_t random_end = random_start + 10;

            int result_count = 0;
            // start time
            auto start_time_scan = std::chrono::system_clock::now();

            kvStore.Scan(random_start, random_end, result_count);

            // end time
            auto end_time_scan = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed_seconds_scan = end_time_scan - start_time_scan;
            double total_time_scan = elapsed_seconds_scan.count();
            sum_total_time_scan += total_time_scan;
        }
        double avg_total_time_scan = 10 / sum_total_time_scan;

        // Write the average times to the file
        output_file << "Iteration " << i + 1 << ":\n";
        output_file << "Average Put Throughput: " << avg_total_time_put << " operations per second\n";
        output_file << "Average Get Throughput (Binary Search): " << avg_total_time_binary << " operations per second\n";
        output_file << "Average Get Throughput (B-Tree): " << avg_total_time_btree << " operations per second\n";
        output_file << "Average Scan Throughput: " << avg_total_time_scan << " operations per second\n";
        output_file << "-----------------------------------------\n";
    }

    // Close the file
    output_file.close();

    // Close the database (final memtable flush to SST)
    kvStore.Close();

    return 0;
}
