#include "query5.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <cstring>

// -------------------------------
// parseArgs implementation
// -------------------------------
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
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
        else return false;
    }

    return !(r_name.empty() || start_date.empty() || end_date.empty()
             || table_path.empty() || result_path.empty() || num_threads <= 0);
}

// -------------------------------
// Fast parsers
// -------------------------------

static inline int fast_atoi(const char* p, size_t len) {
    int v = 0;
    int sign = 1;
    size_t i = 0;
    if (i < len && p[i] == '-') { sign = -1; ++i; }
    for (; i < len; ++i) {
        char c = p[i];
        if (c < '0' || c > '9') break;
        v = v * 10 + (c - '0');
    }
    return v * sign;
}

static inline double fast_atof(const char* p, size_t len) {
    double sign = 1.0;
    size_t i = 0;
    if (i < len && p[i] == '-') { sign = -1.0; ++i; }
    double ipart = 0.0;
    for (; i < len; ++i) {
        char c = p[i];
        if (c < '0' || c > '9') break;
        ipart = ipart * 10.0 + (c - '0');
    }
    if (i < len && p[i] == '.') {
        ++i;
        double place = 0.1;
        for (; i < len; ++i) {
            char c = p[i];
            if (c < '0' || c > '9') break;
            ipart += (c - '0') * place;
            place *= 0.1;
        }
    }
    return sign * ipart;
}

// -------------------------------
// readLineItemParallel - parallel parse (fields 0,2,5,6)
// -------------------------------
struct FileRange {
    size_t start;
    size_t end;
};

static void parseLineItemFastFields(const std::string& line, LineItem& l) {
    const char* base = line.c_str();
    size_t len = line.size();
    size_t prev = 0;
    int field = 0;
    int orderkey = -1;
    int suppkey = -1;
    double extendedprice = 0.0;
    double discount = 0.0;

    for (size_t i = 0; i <= len; ++i) {
        if (i == len || base[i] == '|') {
            size_t tok_len = i - prev;
            const char* tok = base + prev;
            if (field == 0) orderkey = fast_atoi(tok, tok_len);
            else if (field == 2) suppkey = fast_atoi(tok, tok_len);
            else if (field == 5) extendedprice = fast_atof(tok, tok_len);
            else if (field == 6) discount = fast_atof(tok, tok_len);
            prev = i + 1;
            ++field;
        }
    }

    l.orderkey = orderkey;
    l.suppkey = suppkey;
    l.extendedprice = extendedprice;
    l.discount = discount;
}

static bool readLineItemParallel(
    const std::string& path,
    int num_threads,
    std::vector<LineItem>& lineitems
) {
    std::ifstream fs(path, std::ios::binary | std::ios::ate);
    if (!fs) {
        std::cerr << "Cannot open " << path << "\n";
        return false;
    }
    size_t file_size = static_cast<size_t>(fs.tellg());
    fs.close();

    if (file_size == 0) return true;

    std::vector<FileRange> ranges(num_threads);
    size_t chunk = file_size / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        ranges[i].start = i * chunk;
        ranges[i].end = (i == num_threads - 1) ? file_size : (i + 1) * chunk;
    }

    std::ifstream f(path, std::ios::binary);
    if (!f) return false;
    for (int i = 1; i < num_threads; ++i) {
        f.seekg(static_cast<std::streamoff>(ranges[i].start));
        std::string dummy;
        std::getline(f, dummy);
        ranges[i].start = static_cast<size_t>(f.tellg());
    }
    f.close();

    std::vector<std::vector<LineItem>> locals(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::ifstream in(path, std::ios::binary);
            if (!in) return;
            in.seekg(static_cast<std::streamoff>(ranges[t].start));
            std::string line;
            locals[t].reserve(1024);
            size_t pos = ranges[t].start;
            while (pos < ranges[t].end && std::getline(in, line)) {
                pos = static_cast<size_t>(in.tellg());
                if (!line.empty()) {
                    LineItem li{};
                    parseLineItemFastFields(line, li);
                    locals[t].push_back(li);
                }
                if (pos == 0) break;
            }
        });
    }

    for (auto& th : threads) th.join();

    size_t total = 0;
    for (auto &v : locals) total += v.size();
    lineitems.clear();
    lineitems.reserve(total);
    for (auto &v : locals) {
        for (auto &li : v) lineitems.push_back(li);
    }
    return true;
}

// -------------------------------
// Read TPCH tables (single-threaded for small tables)
// -------------------------------
static void split_token_simple(const std::string& s, std::vector<std::string>& out) {
    out.clear();
    size_t start = 0;
    size_t n = s.size();
    for (size_t i = 0; i <= n; ++i) {
        if (i == n || s[i] == '|') {
            out.emplace_back(s.data() + start, i - start);
            start = i + 1;
        }
    }
}

bool readTPCHData(
    const std::string& table_path,
    std::vector<Customer>& customers,
    std::vector<Order>& orders,
    std::vector<LineItem>& lineitems,
    std::vector<Supplier>& suppliers,
    std::vector<Nation>& nations,
    std::vector<Region>& regions
) {
    std::string line;
    std::vector<std::string> toks;

    // CUSTOMER
    {
        std::ifstream f(table_path + "/customer.tbl");
        if (!f) { std::cerr << "Cannot open customer.tbl\n"; return false; }
        customers.clear();
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            split_token_simple(line, toks);
            if (toks.size() < 4) continue;
            Customer c;
            c.custkey = std::stoi(toks[0]);
            c.nationkey = std::stoi(toks[3]);
            customers.push_back(c);
        }
    }

    // ORDERS
    {
        std::ifstream f(table_path + "/orders.tbl");
        if (!f) { std::cerr << "Cannot open orders.tbl\n"; return false; }
        orders.clear();
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            split_token_simple(line, toks);
            if (toks.size() < 6) continue;
            Order o;
            o.orderkey = std::stoi(toks[0]);
            o.custkey = std::stoi(toks[1]);
            o.orderdate = toks[4];
            orders.push_back(o);
        }
    }

    // LINEITEM (parallel)
    {

        // int parse_threads = std::max(1u, std::thread::hardware_concurrency());
       //int parse_threads = 1;  // force single-thread parsing for WSL stability
        int parse_threads = std::max(1u, std::thread::hardware_concurrency());
        if (!readLineItemParallel(table_path + "/lineitem.tbl", parse_threads, lineitems)) {
            std::cerr << "Failed to read lineitem.tbl\n";
            return false;
        }
    }

    // SUPPLIER
    {
        std::ifstream f(table_path + "/supplier.tbl");
        if (!f) { std::cerr << "Cannot open supplier.tbl\n"; return false; }
        suppliers.clear();
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            split_token_simple(line, toks);
            if (toks.size() < 4) continue;
            Supplier s;
            s.suppkey = std::stoi(toks[0]);
            s.nationkey = std::stoi(toks[3]);
            suppliers.push_back(s);
        }
    }

    // NATION
    {
        std::ifstream f(table_path + "/nation.tbl");
        if (!f) { std::cerr << "Cannot open nation.tbl\n"; return false; }
        nations.clear();
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            split_token_simple(line, toks);
            if (toks.size() < 3) continue;
            Nation n;
            n.nationkey = std::stoi(toks[0]);
            n.name = toks[1];
            n.regionkey = std::stoi(toks[2]);
            nations.push_back(n);
        }
    }

    // REGION
    {
        std::ifstream f(table_path + "/region.tbl");
        if (!f) { std::cerr << "Cannot open region.tbl\n"; return false; }
        regions.clear();
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            split_token_simple(line, toks);
            if (toks.size() < 2) continue;
            Region r;
            r.regionkey = std::stoi(toks[0]);
            r.name = toks[1];
            regions.push_back(r);
        }
    }

    return true;
}

// -------------------------------
// Execute Query 5 optimized: use dense vectors for lookups
// -------------------------------
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
) {
    // find region key
    int region_key = -1;
    for (const auto& r : regions) {
        if (r.name == r_name) { region_key = r.regionkey; break; }
    }
    if (region_key == -1) {
        std::cerr << "Region not found: " << r_name << "\n";
        return false;
    }

    // nationkey -> index for nations in the region, and collect names
    int max_nk = 0;
    for (const auto& n : nations) max_nk = std::max(max_nk, n.nationkey);
    if (max_nk < 0) return false;
    std::vector<int> nation_to_index(static_cast<size_t>(max_nk) + 1, -1);
    std::vector<std::string> nation_names;
    int ni = 0;
    for (const auto& n : nations) {
        if (n.regionkey == region_key) {
            nation_to_index[static_cast<size_t>(n.nationkey)] = ni++;
            nation_names.push_back(n.name);
        }
    }
    if (nation_names.empty()) {
        // nothing to compute
        return true;
    }

    // Determine maximum keys to allocate dense vectors
    int max_orderkey = 0;
    for (const auto& o : orders) max_orderkey = std::max(max_orderkey, o.orderkey);
    int max_custkey = 0;
    for (const auto& c : customers) max_custkey = std::max(max_custkey, c.custkey);
    int max_suppkey = 0;
    for (const auto& s : suppliers) max_suppkey = std::max(max_suppkey, s.suppkey);

    // allocate dense lookup vectors (initialized with -1 meaning not present)
    std::vector<int> order_to_cust(static_cast<size_t>(max_orderkey) + 1, -1);
    for (const auto& o : orders) {
        if (o.orderdate >= start_date && o.orderdate < end_date) {
            if (o.orderkey >= 0 && o.orderkey <= max_orderkey)
                order_to_cust[static_cast<size_t>(o.orderkey)] = o.custkey;
        }
    }

    std::vector<int> cust_to_nation(static_cast<size_t>(max_custkey) + 1, -1);
    for (const auto& c : customers) {
        if (c.custkey >= 0 && c.custkey <= max_custkey)
            cust_to_nation[static_cast<size_t>(c.custkey)] = c.nationkey;
    }

    std::vector<int> supp_to_nation(static_cast<size_t>(max_suppkey) + 1, -1);
    for (const auto& s : suppliers) {
        if (s.suppkey >= 0 && s.suppkey <= max_suppkey) {
            int nk = s.nationkey;
            if (nk >= 0 && nk <= max_nk && nation_to_index[static_cast<size_t>(nk)] != -1) {
                // only suppliers in the selected region (their nation is in region)
                supp_to_nation[static_cast<size_t>(s.suppkey)] = nk;
            }
        }
    }

    // per-thread local accumulators
    if (num_threads <= 0) num_threads = 1;
    size_t n = lineitems.size();
    size_t chunk = (n + static_cast<size_t>(num_threads) - 1) / static_cast<size_t>(num_threads);

    std::vector<std::vector<double>> local(static_cast<size_t>(num_threads),
                                           std::vector<double>(nation_names.size(), 0.0));

    auto tstart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(num_threads));

    auto worker = [&](int tid, size_t b, size_t e) {
        auto &acc = local[static_cast<size_t>(tid)];
        for (size_t i = b; i < e; ++i) {
            const auto &li = lineitems[i];
            if (li.orderkey < 0 || static_cast<size_t>(li.orderkey) > order_to_cust.size() - 1) continue;
            int cust = order_to_cust[static_cast<size_t>(li.orderkey)];
            if (cust == -1) continue;
            if (cust < 0 || static_cast<size_t>(cust) > cust_to_nation.size() - 1) continue;
            int cust_nk = cust_to_nation[static_cast<size_t>(cust)];
            if (cust_nk == -1) continue;
            if (li.suppkey < 0 || static_cast<size_t>(li.suppkey) > supp_to_nation.size() - 1) continue;
            int supp_nk = supp_to_nation[static_cast<size_t>(li.suppkey)];
            if (supp_nk == -1) continue;
            if (cust_nk != supp_nk) continue;
            int idx = nation_to_index[static_cast<size_t>(cust_nk)];
            if (idx == -1) continue;
            double revenue = li.extendedprice * (1.0 - li.discount);
            acc[static_cast<size_t>(idx)] += revenue;
        }
    };

    for (int t = 0; t < num_threads; ++t) {
        size_t b = static_cast<size_t>(t) * chunk;
        size_t e = std::min(n, b + chunk);
        if (b >= e) {
            threads.emplace_back([](){});
        } else {
            threads.emplace_back(worker, t, b, e);
        }
    }

    for (auto &th : threads) th.join();

    auto tend = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> ms = tend - tstart;

    // combine results
    results.clear();
    for (size_t i = 0; i < nation_names.size(); ++i) {
        double sum = 0.0;
        for (int t = 0; t < num_threads; ++t) sum += local[static_cast<size_t>(t)][i];
        results[nation_names[i]] = sum;
    }

    std::cout << "Threads: " << num_threads << "\n";
    std::cout << "Execution time: " << static_cast<long long>(ms.count()) << " ms\n";
    std::cout.flush();
    return true;
}

// -------------------------------
// Output
// -------------------------------
bool outputResults(
    const std::string& result_path,
    const std::map<std::string, double>& results
) {
#ifdef _WIN32
    std::string cmd = "mkdir \"" + result_path + "\" 2>nul";
#else
    std::string cmd = "mkdir -p \"" + result_path + "\" >/dev/null 2>&1";
#endif
    std::system(cmd.c_str());

    std::ofstream out(result_path + "/query5_result.txt");
    if (!out) return false;

    std::vector<std::pair<std::string, double>> sorted(results.begin(), results.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    for (const auto& p : sorted) out << p.first << "|" << p.second << "\n";
    return true;
}