# Domination Score Calculator

This C++ program computes domination scores for a dataset of points in 4-dimensional integer space, using a grid-based approach.

## Overview

- **Input:** A text file where each line contains:  
  `id attr1 attr2 attr3 attr4`  
  (All columns are whitespace-separated integers.)
- **Output:** A file `output.txt` with two columns:  
  `id` and `domination_score`

## Domination Score

A point _A_ "dominates" point _B_ if _A_ is no worse than _B_ in every attribute and strictly better in at least one.

The program aggregates points into grid cells, then efficiently computes, for each point, how many other points it dominates.

## Usage

Compile with a C++17 (or later) compiler:

```sh
g++ -std=c++17 -O2 -o domination main.cpp
```

Run:

```sh
domination.exe input_file [gridSize0 gridSize1 gridSize2 gridSize3]
```

- `input_file`: Path to your dataset.
- `gridSize0` ... `gridSize3`: (Optional) Number of grid cells per attribute (default: 25 for each).

Example:

```sh
domination.exe data.txt 30 30 30 30
```

## Output

- `output.txt` will be created in the current directory.
- Each line:  
  `id <tab> domination_score`

## File Structure

- `main.cpp`: Main source code.
- `readme.md`: This documentation.

## Notes

- The program expects each line in the input file to have exactly 5 columns: id and 4 attributes.
- The grid size affects performance and accuracy: larger grids are more precise but slower.
- The code is designed for datasets with integer attributes.
