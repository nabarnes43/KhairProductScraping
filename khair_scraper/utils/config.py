"""
Configuration Module for Khair Scraper

This module provides centralized configuration settings for the scraper.
"""

from datetime import datetime

# Global configuration (can be overridden via command line args)
CONFIG = {
    "SKINSORT_JSON": "all_products.json",  # Reference product list
    "CHECKPOINT_FILE": "spider_checkpoint.pkl",  # Checkpoint for resuming
    "CACHE_FILE": "new_cache.json",         # Cache file in root directory
    "PAGES_PER_BATCH": 20,                      # Pages to crawl before pausing
    "TOTAL_PAGES": 3094,                        # Total pages to crawl
    "PAUSE_MINUTES": 0,                         # Pause duration between batches
    "SIMILARITY_THRESHOLD": 90,                 # Matching threshold
    "BATCH_SIZE": 20,                         # Products per output batch scales with pages per batch 50 products per page
    "OUTPUT_DIR": f'products_{datetime.now().strftime("%Y%m%d_%H%M%S")}',  # Timestamped output dir
    "RESOURCE_CHECK_ENABLED": True,             # Monitor system resources
    "MAX_MEMORY_PERCENT": 90,                   # Max memory usage before pause
    "MAX_DISK_PERCENT": 90,                     # Max disk usage before pause
}

# Scrapy spider settings
SPIDER_SETTINGS = {
    # Concurrency settings
    'CONCURRENT_REQUESTS': 2,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
    'DOWNLOAD_DELAY': 2,
    
    # Autothrottle for adaptive rate control
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY': 2,
    'AUTOTHROTTLE_MAX_DELAY': 60,
    'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
    'AUTOTHROTTLE_DEBUG': True,
    
    # Retry settings
    'RETRY_ENABLED': True,
    'RETRY_TIMES': 3,
    'RETRY_HTTP_CODES': [500, 502, 503, 504, 429, 403],
    'RETRY_BASE_DELAY': 2,
    'RETRY_MAX_DELAY': 60,
    
    # User agent rotation and headers
    'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'DEFAULT_REQUEST_HEADERS': {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    },
    
    # Output settings
    'FEED_EXPORT_ENCODING': 'utf-8',
    
    # Handle errors gracefully
    'HTTPERROR_ALLOW_ALL': True,
} 