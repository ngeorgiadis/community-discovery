#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <array>
#include <numeric>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <limits>
#include <cstdint>

using namespace std;

// --------------------------------------------------------------------------
// Type alias for 4‑element arrays (the attributes).
using Int4 = array<int64_t, 4>;

// --------------------------------------------------------------------------
// Custom hash and equality for Int4 (so we can use them as keys in unordered_map).
struct Int4Hash
{
    size_t operator()(const Int4 &k) const
    {
        size_t res = 0;
        for (int i = 0; i < 4; i++)
        {
            // A variant of boost::hash_combine.
            res ^= hash<int64_t>()(k[i]) + 0x9e3779b97f4a7c15ULL + (res << 6) + (res >> 2);
        }
        return res;
    }
};

struct Int4Equal
{
    bool operator()(const Int4 &a, const Int4 &b) const
    {
        for (int i = 0; i < 4; i++)
        {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }
};

struct DatasetRow
{
    int64_t id;
    string name; // (empty string, since input has no label)
    Int4 attrs;  // 4 attributes (pc, cn, hi, pi)
};

struct DatasetStats
{
    int64_t count;
    Int4 max;
    Int4 min;
    // For each attribute (index 0 to 3), a histogram: attribute value -> count.
    array<unordered_map<int64_t, int64_t>, 4> histogram;
};

struct DatasetPoint
{
    Int4 attrs;
    int64_t count;
};

struct DatasetOutput
{
    // Map from id to the corresponding row.
    unordered_map<int64_t, DatasetRow> rows;
    DatasetStats stats;
    // Unique data points (aggregated by the 4‑element key).
    vector<DatasetPoint> points;
};

// Returns true if every element of a is strictly less than b.
inline bool a_less_b(const Int4 &a, const Int4 &b)
{
    for (int i = 0; i < 4; i++)
    {
        if (a[i] >= b[i])
            return false;
    }
    return true;
}

// Returns true if a is less than or equal to b component–wise.
inline bool a_less_or_equal_b(const Int4 &a, const Int4 &b)
{
    for (int i = 0; i < 4; i++)
    {
        if (a[i] > b[i])
            return false;
    }
    return true;
}

// Returns true if a "dominates" b – that is, a is no worse than b in every coordinate
// and is strictly better in at least one.
inline bool a_dominates_b(const Int4 &a, const Int4 &b)
{
    bool at_least_one_better = false;
    for (int i = 0; i < 4; i++)
    {
        if (a[i] < b[i])
            return false;
        if (a[i] > b[i])
            at_least_one_better = true;
    }
    return at_least_one_better;
}

// Sum of all elements in an Int4.
inline int64_t sumInt4(const Int4 &v)
{
    return v[0] + v[1] + v[2] + v[3];
}

// --------------------------------------------------------------------------
// translateDatasetPoint: exactly as in Julia
// Julia code:
//    for (i, v) in enumerate(p)
//         steps = (stats.max[i] - stats.min[i]) / gridSize[i]
//         res[i] = 1 + trunc(Int64, v / steps)
//    end
// [NOTE]: There is NO subtraction of stats.min[i] in the division.
// --------------------------------------------------------------------------
inline Int4 translateDatasetPoint(const Int4 &p, const DatasetStats &stats, const Int4 &gridSize)
{
    Int4 res;
    for (int i = 0; i < 4; i++)
    {
        double steps = double(stats.max[i] - stats.min[i]) / gridSize[i];
        if (steps == 0)
            res[i] = 1;
        else
            res[i] = 1 + static_cast<int64_t>(p[i] / steps); // truncation toward zero
    }
    return res;
}

// --------------------------------------------------------------------------
// Comparison function for DatasetPoint sorting.
// In Julia:
//    if s1 > s2 return true; elseif s1 == s2, return (std(x) < std(y)); else false.
// IMPORTANT: Julia’s std(x) uses the sample standard deviation, i.e. division by (n-1)
// For a 4-element vector, that denominator is 3.0.
// --------------------------------------------------------------------------
bool datasetpointSortFn(const DatasetPoint &x, const DatasetPoint &y)
{
    int64_t s1 = sumInt4(x.attrs);
    int64_t s2 = sumInt4(y.attrs);
    if (s1 > s2)
        return true;
    else if (s1 == s2)
    {
        double mean_x = s1 / 4.0, mean_y = s2 / 4.0;
        double var_x = 0.0, var_y = 0.0;
        for (int i = 0; i < 4; i++)
        {
            var_x += (x.attrs[i] - mean_x) * (x.attrs[i] - mean_x);
            var_y += (y.attrs[i] - mean_y) * (y.attrs[i] - mean_y);
        }
        // Use sample variance (divide by 3 for n=4) as in Julia’s std.
        double std_x = sqrt(var_x / 3.0);
        double std_y = sqrt(var_y / 3.0);
        return std_x < std_y;
    }
    return false;
}

// --------------------------------------------------------------------------
// read_dataset: reads the input text file.
// Each line is assumed to contain: id attr1 attr2 attr3 attr4
// Columns are whitespace–separated.
// --------------------------------------------------------------------------
DatasetOutput read_dataset(const string &input_file)
{
    ifstream infile(input_file);
    if (!infile)
    {
        cerr << "Could not open file: " << input_file << endl;
        exit(1);
    }

    DatasetOutput output;
    DatasetStats stats;
    stats.count = 0;
    stats.max = {numeric_limits<int64_t>::min(),
                 numeric_limits<int64_t>::min(),
                 numeric_limits<int64_t>::min(),
                 numeric_limits<int64_t>::min()};
    stats.min = {numeric_limits<int64_t>::max(),
                 numeric_limits<int64_t>::max(),
                 numeric_limits<int64_t>::max(),
                 numeric_limits<int64_t>::max()};
    // Initialize histograms.
    for (int i = 0; i < 4; i++)
        stats.histogram[i] = unordered_map<int64_t, int64_t>();

    // Use Int4 as key to accumulate unique data points.
    unordered_map<Int4, int64_t, Int4Hash, Int4Equal> uniquePoints;

    string line;
    while (getline(infile, line))
    {
        if (line.empty())
            continue;
        istringstream iss(line);
        int64_t id;
        if (!(iss >> id))
        {
            cerr << "Error reading id from line: " << line << endl;
            continue;
        }
        Int4 attrs;
        for (int i = 0; i < 4; i++)
        {
            if (!(iss >> attrs[i]))
            {
                cerr << "Invalid number of data points in line: " << line << endl;
                goto next_line; // skip this line
            }
        }
        {
            // Update per–attribute min/max and histogram.
            for (int i = 0; i < 4; i++)
            {
                if (attrs[i] > stats.max[i])
                    stats.max[i] = attrs[i];
                if (attrs[i] < stats.min[i])
                    stats.min[i] = attrs[i];
                stats.histogram[i][attrs[i]]++;
            }
            uniquePoints[attrs]++; // Count occurrences.
            DatasetRow row{id, "", attrs};
            output.rows[id] = row;
        }
    next_line:;
    }
    infile.close();

    stats.count = output.rows.size();
    output.stats = stats;

    output.points.reserve(uniquePoints.size());
    for (const auto &kv : uniquePoints)
    {
        output.points.push_back(DatasetPoint{kv.first, kv.second});
    }

    return output;
}

// --------------------------------------------------------------------------
// calc_domination_score: Computes the domination score (exactly as in Julia)
// using a grid–based approach. The grid size is now passed as a parameter.
// --------------------------------------------------------------------------
unordered_map<Int4, int64_t, Int4Hash, Int4Equal>
calc_domination_score(const DatasetStats &stats, vector<DatasetPoint> points, const Int4 &gridSize, bool verbose = true)
{
    auto start = chrono::high_resolution_clock::now();

    // Sort the unique points using our custom comparator.
    sort(points.begin(), points.end(), datasetpointSortFn);

    unordered_map<Int4, int64_t, Int4Hash, Int4Equal> domination;
    unordered_map<Int4, vector<DatasetPoint>, Int4Hash, Int4Equal> grid;

    // Assign each unique point to its grid cell using the provided gridSize.
    for (auto &v : points)
    {
        Int4 coordinates = translateDatasetPoint(v.attrs, stats, gridSize);
        grid[coordinates].push_back(v);
    }

    // Build a vector of grid cell coordinates (as DatasetPoint, with count=0).
    vector<DatasetPoint> gridCoords;
    gridCoords.reserve(grid.size());
    for (const auto &kv : grid)
    {
        gridCoords.push_back(DatasetPoint{kv.first, 0});
    }

    // Sort gridCoords using the same comparator.
    sort(gridCoords.begin(), gridCoords.end(), datasetpointSortFn);

    if (verbose)
        cout << "Total cells to explore: " << gridCoords.size() << endl;

    // Precompute the sum (of grid coordinates) for each grid cell.
    vector<int64_t> gridSums(gridCoords.size());
    for (size_t i = 0; i < gridCoords.size(); i++)
    {
        gridSums[i] = sumInt4(gridCoords[i].attrs);
    }

    // Main double–loop over grid cells.
    for (size_t i = 0; i < gridCoords.size(); i++)
    {
        const DatasetPoint &c = gridCoords[i];
        const Int4 &point = c.attrs;
        int64_t sumPoint = gridSums[i];
        int64_t baseScore = 0;
        vector<DatasetPoint> later;
        later.reserve(gridCoords.size() - i);

        // Loop 1: iterate over grid cells from index i to end.
        for (size_t j = i; j < gridCoords.size(); j++)
        {
            if (gridSums[j] > sumPoint)
                continue;
            const Int4 &pt = gridCoords[j].attrs;
            // Exactly as in Julia: if a_less_b(pt, point) then add all counts
            // from that grid cell; else if a_less_or_equal_b(pt, point) then append.
            if (a_less_b(pt, point))
            {
                for (const auto &dp : grid[pt])
                    baseScore += dp.count;
            }
            else if (a_less_or_equal_b(pt, point))
            {
                const auto &vec = grid[pt];
                later.insert(later.end(), vec.begin(), vec.end());
            }
        }

        // Loop 2: for each point in the current grid cell, add extra scores.
        const auto &currentCell = grid[c.attrs];
        for (const auto &n : currentCell)
        {
            int64_t nodeScore = baseScore;
            for (const auto &l : later)
            {
                if (a_dominates_b(n.attrs, l.attrs))
                    nodeScore += l.count;
            }
            domination[n.attrs] = nodeScore;
        }

        if (verbose && (i % 100 == 0))
            cout << ".";
    }

    if (verbose)
    {
        auto end = chrono::high_resolution_clock::now();
        double elapsed = chrono::duration_cast<chrono::milliseconds>(end - start).count() / 1000.0;
        cout << "\n\nTIME: domination score found in " << elapsed << " seconds" << endl;
    }

    return domination;
}

// --------------------------------------------------------------------------
// main()
// Now accepts command line arguments for the input file and for the grid sizes.
// Usage:
//    ./program input_file [gridSize0 gridSize1 gridSize2 gridSize3]
// If grid sizes are not provided, default values (25) are used.
// The output file is written as "output.txt" (with two columns: id and domination_score).
// --------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        cout << "Usage: " << argv[0] << " input_file [gridSize0 gridSize1 gridSize2 gridSize3]" << endl;
        return 1;
    }

    string input_file = argv[1];
    // Set default grid sizes.
    Int4 gridSize = {25, 25, 25, 25};
    if (argc == 6)
    {
        try
        {
            gridSize[0] = stoll(argv[2]);
            gridSize[1] = stoll(argv[3]);
            gridSize[2] = stoll(argv[4]);
            gridSize[3] = stoll(argv[5]);
        }
        catch (...)
        {
            cerr << "Error: grid sizes must be integer values." << endl;
            return 1;
        }
    }
    else if (argc != 2)
    {
        cout << "Usage: " << argv[0] << " input_file [gridSize0 gridSize1 gridSize2 gridSize3]" << endl;
        return 1;
    }

    string output_file = "output.txt";

    // Read dataset.
    DatasetOutput data = read_dataset(input_file);

    // Compute domination scores using the provided grid sizes.
    auto domination = calc_domination_score(data.stats, data.points, gridSize, true);

    // Open output file.
    ofstream outfile(output_file);
    if (!outfile)
    {
        cerr << "Could not open output file: " << output_file << endl;
        return 1;
    }
    outfile << "id\tdomination_score\n";

    // To ensure consistent ordering, sort rows by id.
    vector<DatasetRow> rows;
    rows.reserve(data.rows.size());
    for (const auto &kv : data.rows)
        rows.push_back(kv.second);
    sort(rows.begin(), rows.end(), [](const DatasetRow &a, const DatasetRow &b)
         { return a.id < b.id; });

    // For each row, look up its domination score by using its attribute vector as key.
    for (const auto &row : rows)
    {
        int64_t score = 0;
        auto it = domination.find(row.attrs);
        if (it != domination.end())
            score = it->second;
        outfile << row.id << "\t" << score << "\n";
    }

    outfile.close();
    cout << "Output written to " << output_file << endl;

    return 0;
}
