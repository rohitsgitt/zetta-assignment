#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Function to parse command line arguments


bool parseArgs(
    int argc,
    char* argv[],
    std::string& r_name,
    std::string& start_date,
    std::string& end_date,
    int& num_threads,
    std::string& table_path,
    std::string& result_path
) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        } else {
            std::cerr << "Unknown or incomplete argument: " << arg << std::endl;
            return false;
        }
    }

    // Basic validation
    if (r_name.empty() || start_date.empty() || end_date.empty() ||
        table_path.empty() || result_path.empty() || num_threads <= 0) {
        return false;
    }

    return true;
}

// Function to read TPCH data from the specified paths


static bool readTable(
    const std::string& file_path,
    const std::vector<std::string>& columns,
    std::vector<std::map<std::string, std::string>>& data
) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "FAILED to open file: " << file_path << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string field;
        std::map<std::string, std::string> row;

        // Read only required columns
        for (const auto& col : columns) {
            if (!std::getline(ss, field, '|')) {
                // Skip malformed line, but DO NOT fail entire program
                row.clear();
                break;
            }
            row[col] = field;
        }

        if (!row.empty()) {
            data.push_back(std::move(row));
        }
    }

    return true;
}

bool readTPCHData(
    const std::string& table_path,
    std::vector<std::map<std::string, std::string>>& customer_data,
    std::vector<std::map<std::string, std::string>>& orders_data,
    std::vector<std::map<std::string, std::string>>& lineitem_data,
    std::vector<std::map<std::string, std::string>>& supplier_data,
    std::vector<std::map<std::string, std::string>>& nation_data,
    std::vector<std::map<std::string, std::string>>& region_data
) {
    bool ok = true;

    ok &= readTable(
        table_path + "/customer.tbl",
        {"c_custkey", "c_name", "c_address", "c_nationkey"},
        customer_data
    );

    ok &= readTable(
        table_path + "/orders.tbl",
        {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"},
        orders_data
    );

    ok &= readTable(
        table_path + "/lineitem.tbl",
        {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
         "l_quantity", "l_extendedprice", "l_discount"},
        lineitem_data
    );

    ok &= readTable(
        table_path + "/supplier.tbl",
        {"s_suppkey", "s_name", "s_address", "s_nationkey"},
        supplier_data
    );

    ok &= readTable(
        table_path + "/nation.tbl",
        {"n_nationkey", "n_name", "n_regionkey"},
        nation_data
    );

    ok &= readTable(
        table_path + "/region.tbl",
        {"r_regionkey", "r_name"},
        region_data
    );

    return ok;
}


// Function to execute TPCH Query 5 using multithreading


bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::string, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& results
) {
    // 1. Find region key for r_name
    std::string region_key;
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) {
            region_key = r.at("r_regionkey");
            break;
        }
    }
    if (region_key.empty()) return false;

    // 2. Map nationkey -> nation name (only in region)
    std::map<std::string, std::string> nation_map;
    for (const auto& n : nation_data) {
        if (n.at("n_regionkey") == region_key) {
            nation_map[n.at("n_nationkey")] = n.at("n_name");
        }
    }

    // 3. Map suppkey -> nationkey (only valid nations)
    std::map<std::string, std::string> supplier_map;
    for (const auto& s : supplier_data) {
        if (nation_map.count(s.at("s_nationkey"))) {
            supplier_map[s.at("s_suppkey")] = s.at("s_nationkey");
        }
    }

    // 4. Map orderkey -> custkey (date filtered)
    std::map<std::string, std::string> order_map;
    for (const auto& o : orders_data) {
        const std::string& date = o.at("o_orderdate");
        if (date >= start_date && date < end_date) {
            order_map[o.at("o_orderkey")] = o.at("o_custkey");
        }
    }

    // 5. Map custkey -> nationkey
    std::map<std::string, std::string> customer_map;
    for (const auto& c : customer_data) {
        customer_map[c.at("c_custkey")] = c.at("c_nationkey");
    }

    // Mutex for thread-safe aggregation
    std::mutex mtx;

    // Worker function
    auto worker = [&](size_t start, size_t end) {
        std::map<std::string, double> local;

        for (size_t i = start; i < end; ++i) {
            const auto& l = lineitem_data[i];

            auto itOrder = order_map.find(l.at("l_orderkey"));
            if (itOrder == order_map.end()) continue;

            auto itSupp = supplier_map.find(l.at("l_suppkey"));
            if (itSupp == supplier_map.end()) continue;

            const std::string& custkey = itOrder->second;
            auto itCust = customer_map.find(custkey);
            if (itCust == customer_map.end()) continue;

            if (itCust->second != itSupp->second) continue;

            double price = std::stod(l.at("l_extendedprice"));
            double discount = std::stod(l.at("l_discount"));
            double revenue = price * (1.0 - discount);

            const std::string& nation = nation_map[itSupp->second];
            local[nation] += revenue;
        }

        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& p : local) {
            results[p.first] += p.second;
        }
    };

    // 6. Launch threads
    std::vector<std::thread> threads;
    size_t chunk = lineitem_data.size() / num_threads;

    for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk;
        size_t end = (t == num_threads - 1) ? lineitem_data.size() : start + chunk;
        threads.emplace_back(worker, start, end);
    }

    for (auto& th : threads) th.join();

    return true;
}


// Function to output results to the specified path


bool outputResults(
    const std::string& result_path,
    const std::map<std::string, double>& results
) {
    // Ensure directory exists (Windows-safe)
    std::string cmd = "mkdir \"" + result_path + "\" 2>nul";
    system(cmd.c_str());

    std::ofstream out(result_path + "/query5_result.txt");
    if (!out.is_open()) {
        return false;
    }

    // Sort results by revenue DESC
    std::vector<std::pair<std::string, double>> sorted(
        results.begin(), results.end()
    );

    std::sort(sorted.begin(), sorted.end(),
          [](const std::pair<std::string, double>& a,
             const std::pair<std::string, double>& b) {
              return a.second > b.second;
          });

    for (const auto& p : sorted) {
        out << p.first << "|" << p.second << "\n";
    }

    return true;
}
