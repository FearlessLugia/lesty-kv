//
// Created by Kiiro Huang on 2024-11-30.
//

#include "../src/b_tree/b_tree_sstable.h"
#include "../src/database.h"

#include <iostream>
#include <random>
#include <sys/fcntl.h>
#include <unistd.h>
#include <vector>

using namespace std;


// Helper function to generate random data
vector<int64_t> GenerateUniformData(const size_t num_elements, const int64_t range_min, const int64_t range_max) {
    vector<int64_t> data;
    random_device rd;
    mt19937_64 gen(rd());
    uniform_int_distribution<int64_t> dist(range_min, range_max);

    for (size_t i = 0; i < num_elements; ++i) {
        data.push_back(dist(gen));
    }
    return data;
}

// Helper function to generate random queries
vector<int64_t> GenerateQueries(const vector<int64_t> &data, size_t query_size) {
    random_device rd;
    mt19937 gen(rd());

    vector<int64_t> shuffled_data = data;
    shuffle(shuffled_data.begin(), shuffled_data.end(), gen);

    if (query_size > shuffled_data.size()) {
        cerr << "Query size exceeds data size!" << endl;
        return {};
    }
    return vector<int64_t>(shuffled_data.begin(), shuffled_data.begin() + query_size);
}

// Function to measure throughput
double MeasureBinarySearchThroughput(const Database &db, const vector<int64_t> &queries) {
    auto start = chrono::high_resolution_clock::now();

    for (const auto &query: queries) {
        optional<int64_t> result = db.Get(query);

        // Optionally, verify the result
        cout << "Query: " << query << " Result: " << result.value_or(-1) << endl;
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> duration = end - start;

    return queries.size() / duration.count(); // Queries per second
}

int main() {
    cout << "Prepare for experiment 2" << endl;

    // constexpr size_t kMemtableSizeExp = 32 * 1024; // 32KB
    constexpr size_t kMemtableSizeExp = 10 * 1024 * 1024; // 10MB
    constexpr size_t query_count = 1000;
    constexpr size_t kGigaSize = 1024 * 1024 * 1024; // 1GB
    constexpr size_t kPairsForGiga = kGigaSize / kPairSize; // 1GB / 16B = 64M pairs
    constexpr size_t data_step = kPairsForGiga / 10;

    const string db_name = "db_experiment2";
    // Remove the database file if it exists
    // filesystem::remove_all(db_name);

    Database db(kMemtableSizeExp);
    db.Open(db_name);

    for (size_t data_size = data_step; data_size <= kPairsForGiga; data_size += data_step) { // 1GB - 10GB
        // // Generate data
        // vector<int64_t> data = GenerateUniformData(data_size, 1, 1e9);
        // // Insert data
        // for (const auto i: data) {
        //     db.Put(i, i);
        // }
        //
        // // Generate queries
        // vector<int64_t> queries = GenerateQueries(data, query_count);

        vector<int64_t> queries = GenerateUniformData(query_count, 1, 1e9);

        cout << "Start experiment 2" << endl;
        // Measure throughput
        double binary_search_throughput = MeasureBinarySearchThroughput(db, queries);
        cout << "Binary search throughput: " << binary_search_throughput << " queries per second" << endl;


    }

    db.Close();

    cout << "Experiment 2 completed" << endl;
}
