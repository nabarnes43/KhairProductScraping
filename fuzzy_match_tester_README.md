# Fuzzy Match Tester

A utility tool for testing fuzzy matching thresholds on product names against your reference data without running a full scrape.

## Overview

This tool helps you determine the optimal similarity threshold for fuzzy matching by allowing you to:

1. Test individual product names against your skinsort reference data
2. See matches at different threshold levels (95, 90, 85, 80, 75, 70, 65, 60)
3. Analyze how many products would match at each threshold
4. Run in either interactive mode or command-line mode
5. Automatically detect and remove duplicate products from your reference data

## Requirements

- Python 3.6+
- rapidfuzz package

## Installation

1. Ensure you have rapidfuzz installed:
   ```
   pip install rapidfuzz
   ```

2. Make the script executable (Unix/Linux/Mac):
   ```
   chmod +x fuzzy_match_tester.py
   ```

## Usage

The tool now supports two command styles:

### Simple Style (Recommended)

This new style doesn't require quotes around your search text:

```
./fuzzy_match_tester.py Nizoral Anti Dandruff
```

Use a specific reference file:

```
./fuzzy_match_tester.py skinsort_shampoos.json Nizoral Anti Dandruff
```

### Flag Style (Backward Compatible)

The original style with flags still works:

```
./fuzzy_match_tester.py -f skinsort_shampoos.json -t "Nizoral Anti Dandruff"
```

Options:
- `-f, --file`: Path to the reference JSON file (defaults to skinsort_shampoos.json)
- `-t, --text`: Text to match against the reference data

## Important Note About Special Characters

When using the command line, certain characters like `&` have special meaning to the shell. 
If your product name contains these characters, you must either:

1. **Use quotes** around the entire product name:
   ```
   ./fuzzy_match_tester.py "Head & Shoulders Smooth And Silky"
   ```

2. **Escape special characters** with a backslash:
   ```
   ./fuzzy_match_tester.py Head \& Shoulders Smooth And Silky
   ```

3. **Use interactive mode** (recommended for complex queries):
   ```
   ./fuzzy_match_tester.py
   ```
   Then type your query when prompted, without worrying about special characters.

The tool handles special characters internally (converting between "&" and "and"), but
the shell must pass the complete text to the script first.

### Interactive Mode

Run in interactive mode (allows multiple tests):

```
./fuzzy_match_tester.py
```

Or with a specific reference file:

```
./fuzzy_match_tester.py skinsort_shampoos.json
```

In interactive mode, you can:
- Enter product names to test
- Use special syntax for threshold and match count: `product name: t=80 n=10`
- Type 'quit', 'exit', or 'q' to end the session

### Example Output

```
Loaded 30 unique products from skinsort_shampoos.json
Note: Removed 570 duplicate entries

Testing: 'Nizoral Anti Dandruff'
--------------------------------------------------------------------------------
Top 5 matches:
1. [96.55%] ✓ Nizoral Anti-Dandruff Shampoo
2. [90.00%] ✓ Nizoral Ketoconazole 2% Anti-dandruff Shampoo
3. [85.71%]   Nizoral 2% Ketoconazole Shampoo
4. [75.00%]   Nizoral 2% Shampoo
5. [70.00%]   Head & Shoulders Classic Clean Dandruff Shampoo

Threshold Analysis:
--------------------------------------------------------------------------------
Threshold 95: MATCH (Total 1 products would match)
Threshold 90: MATCH (Total 2 products would match)
Threshold 85: MATCH (Total 3 products would match)
Threshold 80: MATCH (Total 3 products would match)
Threshold 75: MATCH (Total 4 products would match)
Threshold 70: MATCH (Total 5 products would match)
Threshold 65: MATCH (Total 8 products would match)
Threshold 60: MATCH (Total 10 products would match)

Recommendations:
--------------------------------------------------------------------------------
High-quality match found! Recommended threshold: 90
--------------------------------------------------------------------------------
```

## New Features

### Duplicate Detection and Removal

The tool now automatically detects and removes duplicate products from the reference data. This ensures more accurate matching and prevents redundant results.

### Enhanced Fuzzy Matching

The tool now employs multiple fuzzy matching algorithms (standard ratio and token set ratio) to find the best possible matches:

- Standard ratio: Good for exact character-by-character matching
- Token set ratio: Better for handling word order differences and partial matches

### Smart Threshold Recommendations

The tool now provides specific recommendations based on the match quality:

- 90+ score: "High-quality match found! Recommended threshold: 90"
- 80-89 score: "Good match found. Recommended threshold: 80"
- 70-79 score: "Fair match found. Recommended threshold: 70"
- Below 70: "Poor match quality. Consider using a lower threshold or improving your query."

## How to Use the Results

The tool provides guidance on which threshold to use:

1. **High precision, lower recall**: Use a threshold of 90-95 if you want only close matches but may miss some valid matches
2. **Balance**: Use a threshold of 85 for a good balance between precision and recall
3. **High recall, lower precision**: Use thresholds of 75-80 if you want to catch more potential matches but might get some false positives

For short product names (less than 15 characters), a higher threshold (90+) is recommended to avoid false matches.

## Notes

- The tool follows similar matching logic as your main scraper
- The ✓ mark indicates matches that would meet a 90+ threshold
- Percentages are now displayed more precisely with decimal points
- The tool's thresholds are tested down to 60% to show more potential matches 