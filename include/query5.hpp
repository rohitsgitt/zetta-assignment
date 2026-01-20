#ifndef QUERY5_HPP
#define QUERY5_HPP

#include <string>
#include <vector>
#include <map>

// DATA STRUCTURES

struct Customer {
    int custkey;
    int nationkey;
};

struct Order {
    int orderkey;
    int custkey;
    std::string orderdate;
};

struct LineItem {
    int orderkey;
    int suppkey;
    double extendedprice;
    double discount;
};

struct Supplier {
    int suppkey;
    int nationkey;
};

struct Nation {
    int nationkey;
    std::string name;
    int regionkey;
};

struct Region {
    int regionkey;
    std::string name;
};

//  FUNCTION DECLARATIONS

// Parse command line arguments
bool parseArgs(
    int argc,
    char* argv[],
    std::string& r_name,
    std::string& start_date,
    std::string& end_date,
    int& num_threads,
    std::string& table_path,
    std::string& result_path
);

// Read TPCH data
bool readTPCHData(
    const std::string& table_path,
    std::vector<Customer>& customers,
    std::vector<Order>& orders,
    std::vector<LineItem>& lineitems,
    std::vector<Supplier>& suppliers,
    std::vector<Nation>& nations,
    std::vector<Region>& regions
);

// Execute TPCH Query 5
bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<Customer>& customers,
    const std::vector<Order>& orders,
    const std::vector<LineItem>& lineitems,
    const std::vector<Supplier>& suppliers,
    const std::vector<Nation>& nations,
    const std::vector<Region>& regions,
    std::map<std::string, double>& results
);

// Output results
bool outputResults(
    const std::string& result_path,
    const std::map<std::string, double>& results
);

#endif // QUERY5_HPP