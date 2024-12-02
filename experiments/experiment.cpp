//
// Created by Kiiro Huang on 2024-11-30.
//

#include "../include/database.h"

#include <iostream>
#include <random>
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
    ranges::shuffle(shuffled_data, gen);

    if (query_size > shuffled_data.size()) {
        cerr << "Query size exceeds data size!" << endl;
        return {};
    }
    return vector<int64_t>(shuffled_data.begin(), shuffled_data.begin() + query_size);
}

double MeasurePutThroughput(Database &db, const size_t data_step) {
    // Generate data
    // For each iteration, insert data_size number of data
    vector<int64_t> data = GenerateUniformData(data_step, 1, 1e9);

    const auto start = chrono::high_resolution_clock::now();
    // Insert data
    for (const auto i: data) {
        db.Put(i, i);
    }

    const auto end = chrono::high_resolution_clock::now();
    const chrono::duration<double> duration = end - start;

    return data.size() / duration.count(); // Inserts per second
}

// Function to measure throughput
double MeasureBinarySearchThroughput(const Database &db, const vector<int64_t> &queries) {
    const auto start = chrono::high_resolution_clock::now();

    for (const auto &query: queries) {
        optional<int64_t> result = db.Get(query);

        // Optionally, verify the result
        // cout << "Query: " << query << " Result: " << result.value_or(-1) << endl;
    }

    const auto end = chrono::high_resolution_clock::now();
    const chrono::duration<double> duration = end - start;

    return queries.size() / duration.count(); // Queries per second
}

double MeasureScanThroughput(Database &db, const vector<int64_t> &queries) {
    const auto start = chrono::high_resolution_clock::now();

    for (const auto &query: queries) {
        vector<pair<int64_t, int64_t>> result = db.Scan(query, query + 100);

        // Optionally, verify the result
    }

    const auto end = chrono::high_resolution_clock::now();
    const chrono::duration<double> duration = end - start;

    return queries.size() / duration.count(); // Queries per second
}

void Experiment() {
    cout << "Prepare for experiment" << endl;

    constexpr size_t query_count = 1000;

    const string db_name = "db_experiment";
    // Remove the database file if it exists
    filesystem::remove_all(db_name);

    Database db(kMemtableSize);
    db.Open(db_name);

    // Initialize output files
    ofstream outPut("experiment_Put.csv");
    outPut << "Data Size,Put Throughput" << endl;

    ofstream outGet("experiment_Get.csv");
    outGet << "Data Size,Binary Search Throughput" << endl;

    ofstream outScan("experiment_Scan.csv");
    outScan << "Data Size,Scan Throughput" << endl;

    constexpr size_t max_exponent = 10;

    // Exponential growth of data size
    for (size_t exp = 0; exp <= max_exponent; ++exp) {
        size_t data_size_mb = 1 << exp; // current MB
        size_t data_size_pairs = (data_size_mb * 1024 * 1024) / 16; // current pairs
        size_t increment_pairs = (exp == 0) ? data_size_pairs : (1 << (exp - 1)) * 1024 * 1024 / 16;

        cout << "data_size_mb " << data_size_mb << endl;
        cout << "data_size_pairs " << data_size_pairs << endl;
        cout << "increment_pairs " << increment_pairs << endl;

        // Measure Put throughput
        // For each iteration, increment data size is the half of current data size
        double put_throughput = MeasurePutThroughput(db, increment_pairs);
        cout << "Put throughput: " << put_throughput << " inserts per second. Data size (MB): " << data_size_mb << endl;
        outPut << data_size_mb << "," << to_string(put_throughput) << endl;

        // Generate queries
        vector<int64_t> queries = GenerateUniformData(query_count, 1, 1e9);

        // Measure Get throughput
        double binary_search_throughput = MeasureBinarySearchThroughput(db, queries);
        cout << "Binary search throughput: " << binary_search_throughput
             << " queries per second. Data size (MB): " << data_size_mb << endl;
        outGet << data_size_mb << "," << to_string(binary_search_throughput) << endl;

        // Measure Scan throughput
        double scan_throughput = MeasureScanThroughput(db, queries);
        cout << "Scan throughput: " << scan_throughput << " queries per second. Data size (MB): " << data_size_mb
             << endl;
        outScan << data_size_mb << "," << to_string(scan_throughput) << std::endl;

        cout << "=====================================" << endl;
    }
    db.Close();

    cout << "Experiment completed" << endl;
}

int main() {
    // When only performing Binary search on B-Tree,
    // Experiment 2 is exactly the same as that of the Get Throughput in Experiment 3

    Experiment();
}
