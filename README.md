# Khair Product Scraping

A comprehensive product scraping system for extracting, analyzing, and matching skincare product information from IncideDecoder.com. This system features automatic resumption, batched processing, resource monitoring, and fuzzy matching capabilities.

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [System Requirements](#system-requirements)
4. [Installation](#installation)
   - [Mac Setup](#mac-setup)
   - [Windows Setup](#windows-setup)
5. [Configuration](#configuration)
6. [Running the Scraper](#running-the-scraper)
7. [Logging System](#logging-system)
8. [Output Structure](#output-structure)
9. [How It Works](#how-it-works)
10. [Troubleshooting](#troubleshooting)

## Overview

The Khair Product Scraping system is designed to crawl IncideDecoder.com for skincare product information and match it against a reference dataset. It features:

- **Batch Processing**: Processes pages in configurable batches with pauses in between
- **Automatic Resumption**: Can resume from where it left off after interruptions
- **Resource Monitoring**: Monitors system resources and pauses when thresholds are exceeded
- **Fuzzy Matching**: Matches product names with a reference dataset using fuzzy string matching
- **Caching**: Caches product information to prevent redundant processing
- **Checkpoint System**: Saves progress at regular intervals
- **Comprehensive Logging**: Detailed logging at multiple levels

## Project Structure

```
KhairProductScraping/
├── khair_scraper/              # Main package
│   ├── __init__.py             # Package initialization
│   ├── crawler_manager.py      # Manages the crawling process
│   ├── main.py                 # Entry point for the application
│   ├── middleware/             # Scrapy middleware components
│   │   ├── __init__.py
│   │   └── retry_middleware.py # Custom retry handling
│   ├── spiders/                # Scrapy spiders
│   │   ├── __init__.py
│   │   └── optimized_product_spider.py  # Main spider implementation
│   └── utils/                  # Utility modules
│       ├── __init__.py
│       ├── config.py           # Configuration settings
│       ├── logging_setup.py    # Logging configuration
│       └── stats_tracker.py    # Statistics tracking
├── output/                     # Default output directory
│   └── [batch_directories]     # Generated batch directories
├── requirements.txt            # Python dependencies
└── README.md                   # This documentation
```

## System Requirements

### Common Requirements
- Python 3.8 or higher
- Scrapy 2.5+
- RapidFuzz (for fuzzy matching)
- Internet connection

### Windows Requirements
- Windows 10/11
- Minimum 4GB RAM
- Visual C++ Build Tools (for some Python dependencies)

### macOS Requirements
- macOS 10.15 (Catalina) or higher
- Minimum 4GB RAM
- Command Line Tools for Xcode (for some Python dependencies)

## Installation

### Mac Setup

```bash
# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### Windows Setup

```bash
# Create a virtual environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

General installation steps:

1. **Clone the repository**

```bash
git clone https://github.com/yourusername/KhairProductScraping.git
cd KhairProductScraping
```

2. **Create a virtual environment (recommended)**

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

## Configuration

The system is configured through the `khair_scraper/utils/config.py` file. Key configuration parameters include:

### General Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `OUTPUT_DIR` | Directory for output files | `output` |
| `CHECKPOINT_FILE` | File for saving progress | `checkpoint.pkl` |
| `CACHE_FILE` | File for product cache | `product_cache.json` |
| `SKINSORT_JSON` | Reference dataset for matching | `skinsort_data.json` |

### Spider Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `TOTAL_PAGES` | Total pages to scrape | `20` |
| `PAGES_PER_BATCH` | Pages to process per batch | `5` |
| `BATCH_SIZE` | Products per batch | `100` |
| `SIMILARITY_THRESHOLD` | Threshold for fuzzy matching (0-100) | `85` |

### Resource Monitoring

| Parameter | Description | Default |
|-----------|-------------|---------|
| `RESOURCE_CHECK_ENABLED` | Enable resource monitoring | `True` |
| `MAX_MEMORY_PERCENT` | Maximum memory usage before pausing | `80` |
| `MAX_DISK_PERCENT` | Maximum disk usage before pausing | `90` |
| `PAUSE_MINUTES` | Minutes to pause between batches | `1` |

## Running the Scraper

To run the scraper with default settings:

```bash
# Windows
python -m khair_scraper.main

# macOS/Linux
python3 -m khair_scraper.main
```

### Command Line Arguments

The system supports various command line arguments:

```bash
python -m khair_scraper.main --output-dir my_output --pages 50 --batch-size 10
```

| Argument | Description |
|----------|-------------|
| `--output-dir` | Output directory path |
| `--pages` | Total pages to scrape |
| `--pages-per-batch` | Pages per batch |
| `--batch-size` | Products per batch |
| `--similarity` | Similarity threshold (0-100) |
| `--skinsort` | Path to reference data JSON |
| `--disable-resource-check` | Disable resource monitoring |
| `--pause-minutes` | Minutes to pause between batches |

## Logging System

The system uses a comprehensive logging approach with multiple log files:

### Log Files

| Log File | Location | Description |
|----------|----------|-------------|
| `scraper.log` | Root directory | Main log file with rotation |
| `OptimizedProductSpider.log` | Output directory | Run-specific log |
| `batch_[number].log` | Batch directory | Batch process log |

### Log Levels

- **DEBUG**: Detailed debugging information
- **INFO**: Confirmation that things are working
- **WARNING**: Indication that something unexpected happened
- **ERROR**: Errors that prevent a function from working
- **CRITICAL**: Critical errors that prevent the program from continuing

## Output Structure

The scraper organizes output in a hierarchical structure:

```
output_dir/
├── batch_1_20250307_172519/       # Batch directory with timestamp
│   ├── batch_1.log                # Batch log
│   ├── batch_checkpoint.pkl       # Batch-specific checkpoint
│   └── crawl_summary.json         # Batch summary with statistics
├── batch_2_20250307_172608/       # Next batch
│   └── ... (similar structure)
├── checkpoint.pkl                 # Global checkpoint
└── OptimizedProductSpider.log     # Global spider log
```

### Output Files

| File | Description |
|------|-------------|
| `product_cache.json` | Cached product information |
| `checkpoint.pkl` | Global checkpoint for resumption |
| `batch_checkpoint.pkl` | Batch-specific checkpoint |
| `crawl_summary.json` | Summary statistics for each batch |

### `crawl_summary.json` Structure

```json
{
  "start_offset": 0,
  "pages_per_batch": 5,
  "batch_number": 1,
  "total_products": 250,
  "matched_products": 120,
  "end_offset": 4,
  "timestamp": "2025-03-07T17:25:19",
  "batch_dir": "output/batch_1_20250307_172519"
}
```

## How It Works

### System Architecture

The system has several key components that work together:

1. **CrawlerManager** (`crawler_manager.py`): Manages the overall crawling process, batching, checkpointing, and resource monitoring

2. **OptimizedProductSpider** (`spiders/optimized_product_spider.py`): The Scrapy spider that extracts and processes product information

3. **GlobalStatsTracker** (`utils/stats_tracker.py`): Tracks and aggregates statistics across batches

### Process Flow

1. **Initialization**:
   - The system loads configuration settings
   - Sets up logging
   - Initializes the crawler manager

2. **Batch Processing**:
   - The crawler manager divides the work into batches
   - For each batch, it creates a timestamped directory
   - The manager runs the spider on each batch with appropriate parameters

3. **Spider Processing**:
   - The spider crawls the IncideDecoder product pages
   - Extracts product information
   - Performs fuzzy matching against the reference dataset
   - Caches processed products
   - Saves progress in checkpoints

4. **Resource Monitoring**:
   - The system monitors memory and disk usage
   - Pauses processing if resource thresholds are exceeded
   - Resumes automatically when resources are available

5. **Checkpointing**:
   - The system saves progress regularly
   - Can resume from the last checkpoint after interruptions
   - Each batch has its own checkpoint to ensure correct processing

6. **Data Aggregation**:
   - The GlobalStatsTracker combines results across batches
   - Generates summary statistics and reports

### Key Algorithms

1. **Fuzzy Matching**: Uses RapidFuzz to match product names against a reference dataset with configurable similarity threshold

2. **Batch Processing**: Processes pages in configurable batches to manage memory usage and allow pauses

3. **Resource Monitoring**: Uses psutil to monitor system resources and prevent overloading

4. **Checkpointing**: Uses pickle to save and restore progress efficiently

## Troubleshooting

### Common Issues

#### Spider only processes one page per batch after the first batch

**Solution**: This problem has been fixed by creating batch-specific checkpoints. Each batch now starts with its own clean checkpoint file with page_count=0.

#### Error writing to run log file

**Solution**: The log_custom method now checks if the run_log_file attribute exists before attempting to use it, preventing this error.

#### High memory usage

**Solution**: Reduce batch sizes and increase pause times between batches.

#### Crawler stops unexpectedly

**Solution**: Check the logs for error messages. Common causes include network issues or resource constraints.

### Debugging Tips

1. **Enable detailed logging**:
   ```bash
   python -m khair_scraper.main --log-level DEBUG
   ```

2. **Check system resources**:
   ```bash
   # Windows
   taskmgr

   # macOS
   top
   ```

3. **Verify checkpoint files**:
   Checkpoint files can be examined (but not directly edited) to verify progress.

4. **Run a small test batch**:
   ```bash
   python -m khair_scraper.main --pages 2 --pages-per-batch 1
   ```

---

For additional support, please file an issue on the GitHub repository.