#include "query5.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <algorithm>

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads = 0;

    if (!parseArgs(argc, argv, r_name, start_date, end_date,
                   num_threads, table_path, result_path)) {
        std::cout << "Usage: --r_name <region> --start_date <YYYY-MM-DD> --end_date <YYYY-MM-DD> --threads <n> --table_path <path> --result_path <path>\n";
        return 1;
    }

    // Cap threads to hardware concurrency to avoid oversubscription
    unsigned hw = std::max(1u, std::thread::hardware_concurrency());
    if (num_threads <= 0) num_threads = 1;
    if ((unsigned)num_threads > hw) {
        num_threads = static_cast<int>(hw);
    }

    std::vector<Customer> customers;
    std::vector<Order> orders;
    std::vector<LineItem> lineitems;
    std::vector<Supplier> suppliers;
    std::vector<Nation> nations;
    std::vector<Region> regions;

    if (!readTPCHData(table_path, customers, orders,
                      lineitems, suppliers, nations, regions)) {
        std::cout << "Failed to read TPCH data.\n";
        return 1;
    }

    std::map<std::string, double> results;

    if (!executeQuery5(r_name, start_date, end_date, num_threads,
                       customers, orders, lineitems,
                       suppliers, nations, regions, results)) {
        std::cout << "Failed to execute TPCH Query 5.\n";
        return 1;
    }

    if (!outputResults(result_path, results)) {
        std::cout << "Failed to output results.\n";
        return 1;
    }

    std::cout << "TPCH Query 5 implementation completed.\n";
    return 0;
}