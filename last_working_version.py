"""
Auto-Resuming Web Scraper with Checkpointing

This script provides a complete solution for running a Scrapy spider with automatic resumption,
checkpointing, and resource monitoring. It consists of two main components:

1. OptimizedProductSpider: An enhanced Scrapy spider for product data extraction
2. CrawlerManager: A wrapper script that manages the spider's execution with pauses

Usage:
    python auto_resuming_scraper.py

The script will automatically run the spider in batches, pausing between them to reduce
server load and avoid getting blocked.
"""

from json_file_cache import JsonFileCache

print("Script starting...")
import sys
import scrapy
from datetime import datetime
import json
import os
import pickle
import re
import random
import time
import subprocess
import sys
import logging
from logging.handlers import RotatingFileHandler
import signal
import traceback
from rapidfuzz import fuzz, process
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

def setup_logging(output_dir=None):
    """Set up a clean logging system with both console and file output"""
    
    # Create formatter for consistent log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Create main file handler for complete log history
    main_log_file = "scraper.log"
    file_handler = RotatingFileHandler(
        main_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Configure root logger for global access
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Create a specific logger for the scraper
    scraper_logger = logging.getLogger("scraper")
    scraper_logger.setLevel(logging.INFO)
    
    # Create a specific logger for the crawler manager
    manager_logger = logging.getLogger("manager")
    manager_logger.setLevel(logging.INFO)
    
    run_log_file = None
    timestamp_str = None # add timestamp_str
    # If an output directory is provided, create a run-specific log file too
    if output_dir:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S') #generate timestamp here
        run_log_file = os.path.join(output_dir, f"run_{timestamp_str}.log")
        run_handler = RotatingFileHandler(
            run_log_file,
            maxBytes=10*1024*1024,
            backupCount=3
        )
        run_handler.setFormatter(formatter)
        # Add the run-specific handler to both loggers

        scraper_logger.addHandler(run_handler)
        manager_logger.addHandler(run_handler)

    return {
        "scraper": scraper_logger,
        "manager": manager_logger,
        "run_log_file": run_log_file,
        "timestamp_str": timestamp_str #return timestamp
    }


# Create global loggers - will be initialized in main function
LOGGERS = None

# Global configuration (can be overridden via command line args)
#Last Page: https://incidecoder.com/products/all?offset=3093
CONFIG = {
    "SKINSORT_JSON": "skinsort_shampoos.json",  # Reference product list
    "CHECKPOINT_FILE": "spider_checkpoint.pkl",  # Checkpoint for resuming
    "CACHE_FILE": "product_cache.json",         # Cache file in root directory
    "PAGES_PER_BATCH": 20,                       # Pages to crawl before pausing
    "TOTAL_PAGES": 3094,                           # Total pages to crawl
    "PAUSE_MINUTES": 0,                         # Pause duration between batches
    "SIMILARITY_THRESHOLD": 95,                 # Matching threshold
    "BATCH_SIZE": 1,                            # Products per output batch
    "OUTPUT_DIR": f'products_{datetime.now().strftime("%Y%m%d_%H%M%S")}',  # Timestamps restored
    "RESOURCE_CHECK_ENABLED": True,             # Monitor system resources
    "MAX_MEMORY_PERCENT": 90,                   # Max memory usage before pause
    "MAX_DISK_PERCENT": 90,                     # Max disk usage before pause
}

class CustomRetryMiddleware(RetryMiddleware):
    """Custom middleware that implements exponential backoff for retries"""
    def __init__(self, settings):
        super(CustomRetryMiddleware, self).__init__(settings)
        self.max_retry_times = settings.getint('RETRY_TIMES', 3)
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES', [500, 502, 503, 504, 429]))
        self.priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST', -1)
        # Base delay in seconds
        self.base_delay = settings.getfloat('RETRY_BASE_DELAY', 1)
        # Maximum delay in seconds
        self.max_delay = settings.getfloat('RETRY_MAX_DELAY', 60)

    def get_retry_request(self, request, reason, spider):
        retry_count = request.meta.get('retry_times', 0) + 1
        
        # Calculate exponential backoff delay
        delay = min(self.max_delay, self.base_delay * (2 ** retry_count))
        # Add jitter to avoid synchronized requests
        delay = delay * (0.5 + random.random())
        
        spider.logger.info(f"Retry {retry_count} for {request.url}, backing off for {delay:.2f} seconds")
        time.sleep(delay)
        
        return super(CustomRetryMiddleware, self).get_retry_request(request, reason, spider)


class OptimizedProductSpider(scrapy.Spider):
    name = 'optimized_products'
    start_urls = ['https://incidecoder.com/products/all']
    
    custom_settings = {
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
        
        # Custom retry middleware
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'auto_resuming_scraper.CustomRetryMiddleware': 550,  
        },
        
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
    
    def __init__(self, skinsort_json=None, similarity_threshold=None, batch_size=None, 
            start_page=None, max_pages=None, checkpoint_file=None, output_dir=None, 
            use_cache=True, cache_file=None, *args, **kwargs):
        """Initialize the spider with configurable parameters and file-based cache"""
        super(OptimizedProductSpider, self).__init__(*args, **kwargs)
            
        # Use provided values or fallback to CONFIG defaults
        self.skinsort_json = skinsort_json or CONFIG["SKINSORT_JSON"]
        self.similarity_threshold = int(similarity_threshold or CONFIG["SIMILARITY_THRESHOLD"])
        self.batch_size = int(batch_size or CONFIG["BATCH_SIZE"])
        self.start_page = int(start_page or 0)
        self.max_pages = int(max_pages or CONFIG["TOTAL_PAGES"])
        self.checkpoint_file = checkpoint_file or CONFIG["CHECKPOINT_FILE"]
        
        
        # Additional config values
        self.resource_check_enabled = CONFIG["RESOURCE_CHECK_ENABLED"]
        self.max_memory_percent = CONFIG["MAX_MEMORY_PERCENT"]
        self.max_disk_percent = CONFIG["MAX_DISK_PERCENT"]
        
        # Set up output directory - WITH timestamp for unique batches
        self.output_dir = output_dir or CONFIG["OUTPUT_DIR"]
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Initialize tracking variables
        self.processed_urls = set()
        self.product_count = 0
        self.matched_count = 0
        self.page_count = 0  # This will be incremented in parse()
        self.batch_count = 0
        self.product_batch = []
        
        # Initialize the last_offset attribute for progress tracking
        self.last_offset = self.start_page
        
        # Load Skinsort data
        self.load_skinsort_data(self.skinsort_json)
        
        #TODO Doubt I need all this
        global LOGGERS
        if not LOGGERS:
            LOGGERS = setup_logging(output_dir)
        self.run_log_file = LOGGERS["run_log_file"]
        self.timestamp_str = LOGGERS["timestamp_str"] #store timestamp
        if self.timestamp_str is None:
            self.timestamp_str = "No_timestamp" #handle case where no output_dir is passed.

        # Initialize file-based cache - ALWAYS in root directory, separate from output_dir
        self.use_cache = use_cache
        if self.use_cache:
            # Use provided cache file or CONFIG default (in root directory)
            cache_file = cache_file or CONFIG["CACHE_FILE"]
            
            # Ensure we use an absolute path if not already
            if not os.path.isabs(cache_file):
                cache_file = os.path.abspath(cache_file)
            
            self.product_cache = JsonFileCache(cache_file=cache_file, save_frequency=10)
            cache_stats = self.product_cache.get_stats()
            
            self.log_custom(f"JSON file cache initialized at {cache_file} with {cache_stats['size']} products")

        # IMPORTANT: Load checkpoint after initializing all values
        self.load_checkpoint()
        
        # Start URLs should be constructed using the start_page (after loading checkpoint)
        self.start_urls = [f'https://incidecoder.com/products/all?offset={self.start_page}']
        self.log_custom(f"Spider initialized with start_page={self.start_page}, max_pages={self.max_pages}")
        self.log_custom(f"Initial URL will be: {self.start_urls[0]}")

    def load_checkpoint(self):
        """Load state from checkpoint file with improved handling"""
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            self.log_custom("No checkpoint file found, starting from scratch", logging.INFO)
            return
            
        try:
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint = pickle.load(f)
                
            # Log the checkpoint content without processed_urls and with variable descriptions
            self.log_custom(f"Loaded checkpoint: product_count={checkpoint.get('product_count', 0)}, "
                            f"matched_count={checkpoint.get('matched_count', 0)}, "
                            f"page_count={checkpoint.get('page_count', 0)}, "
                            f"batch_count={checkpoint.get('batch_count', 0)}, "
                            f"last_offset={checkpoint.get('last_offset', None)}")
            
            # Don't overwrite the start_page that was passed to the constructor
            # We just log the checkpoint data for debugging
            self.log_custom(f"Current start_page (from constructor): {self.start_page}")
            self.log_custom(f"Checkpoint last_offset: {checkpoint.get('last_offset', 'Not found')}")
            
            # Only load the processed_urls from checkpoint to avoid duplicate processing
            self.processed_urls = checkpoint.get('processed_urls', set())
            self.log_custom(f"Loaded {len(self.processed_urls)} already processed URLs from checkpoint")
            
        except Exception as e:
            self.log_custom(f"Error loading checkpoint: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)
            self.log_custom("Starting from beginning due to checkpoint error", logging.WARNING)

    def log_custom(self, message, level=logging.INFO):
            """Single logging method to use throughout the spider"""
            self.logger.log(level, message)
            try:
                if self.run_log_file:
                    with open(self.run_log_file, 'a', encoding='utf-8') as f:
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        f.write(f"{timestamp} - {message}\n")
            except Exception as e:
                self.logger.error(f"Error writing to run log file: {str(e)}")
        
    def load_skinsort_data(self, skinsort_json):
        """Load product data from Skinsort JSON file"""
        self.skinsort_products = []
        if not skinsort_json:
            raise ValueError("Skinsort JSON file must be provided with -a skinsort_json=filename.json")
            
        try:
            with open(skinsort_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Validate the data structure
                if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    if all('brand' in item and 'name' in item for item in data):
                        # Create a list of full product names for matching
                        self.skinsort_products = [f"{p['brand']} {p['name']}" for p in data]
                        # Store categories for each product
                        self.product_categories = {f"{p['brand']} {p['name']}": p.get('category', '') for p in data}
                    else:
                        raise ValueError("JSON list items must contain 'brand' and 'name' fields")
                else:
                    raise ValueError(f"Expected a list of product objects, got: {type(data)}")
                
            self.log_custom(f"Loaded {len(self.skinsort_products)} products from Skinsort JSON")
            # Log a sample of products for verification
            if self.skinsort_products:
                self.log_custom(f"Sample products: {self.skinsort_products[:3]}")
        except Exception as e:
            error_msg = f"Error loading Skinsort JSON: {str(e)}"
            self.log_custom(error_msg, logging.ERROR)
            raise ValueError(error_msg)
    
    def load_checkpoint(self):
        """Load state from checkpoint file with improved handling"""
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            self.log_custom("No checkpoint file found, starting from scratch", logging.INFO)
            return
            
        try:
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint = pickle.load(f)
                
          # Log the checkpoint content without processed_urls and with variable descriptions
            self.log_custom(f"Loaded checkpoint: product_count={checkpoint.get('product_count', 0)}, "
                            f"matched_count={checkpoint.get('matched_count', 0)}, "
                            f"page_count={checkpoint.get('page_count', 0)}, "
                            f"batch_count={checkpoint.get('batch_count', 0)}, "
                            f"last_offset={checkpoint.get('last_offset', None)}")
            self.log_custom(f"Loaded checkpoint with {len(self.processed_urls)} already processed URLs")

            # Restore state from checkpoint
            self.processed_urls = checkpoint.get('processed_urls', set())
            self.product_count = checkpoint.get('product_count', 0)
            self.matched_count = checkpoint.get('matched_count', 0)
            self.page_count = checkpoint.get('page_count', 0)
            self.batch_count = checkpoint.get('batch_count', 0)
            
            # Handle the last_offset case - This is the critical part
            if 'last_offset' in checkpoint:
                self.last_offset = checkpoint['last_offset']
                next_offset = self.last_offset + 1
                self.start_page = next_offset
                self.log_custom(f"Resuming from offset {next_offset} (page {next_offset + 1})")
                self.start_urls = [f'https://incidecoder.com/products/all?offset={next_offset}']
            else:
                self.log_custom("No last_offset found in checkpoint, starting from beginning", logging.WARNING)
            
        except Exception as e:
            self.log_custom(f"Error loading checkpoint: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)
            self.log_custom("Starting from beginning due to checkpoint error", logging.WARNING)
        
    def save_checkpoint_safe(self, current_offset=None):
        """Save crawler state with proper value persistence"""
        if not self.checkpoint_file:
            self.log_custom("No checkpoint file specified, skipping checkpoint")
            return
        
        # Create backup of existing checkpoint first
        if os.path.exists(self.checkpoint_file):
            backup_file = f"{self.checkpoint_file}.bak"
            try:
                import shutil
                shutil.copy2(self.checkpoint_file, backup_file)
                self.log_custom(f"Created checkpoint backup at {backup_file}")
            except Exception as e:
                self.log_custom(f"Could not create backup checkpoint: {str(e)}", logging.WARNING)
        
        # Store the current offset and make it a persistent attribute of the class
        if current_offset is not None:
            self.last_offset = current_offset
        
        # Prepare checkpoint data - Use the class attribute to ensure persistence
        checkpoint = {
            'processed_urls': self.processed_urls,
            'product_count': self.product_count,
            'matched_count': self.matched_count,
            'page_count': self.page_count,
            'batch_count': self.batch_count,
            'last_offset': self.last_offset,
            'timestamp': datetime.now().isoformat()
        }
        
        self.log_custom(f"Saving checkpoint with offset={checkpoint['last_offset']}, page_count={self.page_count}")
        
        # Atomic write with safer error handling
        temp_file = f"{self.checkpoint_file}.tmp"
        try:
            with open(temp_file, 'wb') as f:
                pickle.dump(checkpoint, f)
            
            # On Windows, we can't directly rename over an existing file
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
            os.rename(temp_file, self.checkpoint_file)
            
            self.log_custom(f"Successfully saved checkpoint to {self.checkpoint_file}")
        except Exception as e:
            self.log_custom(f"Error saving checkpoint: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)

    def save_batch(self):
        """Save current batch of products to disk and clear memory"""
        if not self.product_batch:
            return
            
        self.batch_count += 1
        filename = os.path.join(self.output_dir, f'product_batch_{self.batch_count}.json')
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.product_batch, f, indent=2, ensure_ascii=False)
                
            self.log_custom(f"Saved batch {self.batch_count} with {len(self.product_batch)} products to {filename}")
            # Clear memory after saving
            self.product_batch = []
        except Exception as e:
            self.log_custom(f"Error saving batch {self.batch_count}: {str(e)}", logging.ERROR)
            # Try to save to an alternative location
            alt_filename = os.path.join(self.output_dir, f'product_batch_{self.batch_count}_emergency.json')
            try:
                with open(alt_filename, 'w', encoding='utf-8') as f:
                    json.dump(self.product_batch, f, indent=2, ensure_ascii=False)
                self.log_custom(f"Saved emergency batch to {alt_filename}")
                self.product_batch = []
            except:
                self.log_custom(f"Failed to save batch {self.batch_count} to any location", logging.CRITICAL)

    def parse(self, response):
        """Parse the product listing page with improved cache utilization"""
        self.page_count += 1
        
        # Extract current offset from URL (represents page number)
        current_offset = 0
        if 'offset=' in response.url:
            offset_match = re.search(r'offset=(\d+)', response.url)
            if offset_match:
                current_offset = int(offset_match[1])
        
        # Store this for checkpoint saving - critical for progress tracking
        self.last_offset = current_offset
        
        self.log_custom(f"\n{'='*50}")
        self.log_custom(f"Processing page {current_offset + 1} (offset={current_offset}): {response.url}")
        self.log_custom(f"Expected products: {current_offset * 50 + 1}-{(current_offset + 1) * 50}")
        self.log_custom(f"Actual page count (internal counter): {self.page_count}")
        
        # Save checkpoint with the current offset
        self.save_checkpoint_safe(current_offset)
        
        # Find all product links on the page
        links = response.xpath('//a[contains(@href, "/products/")]')
        self.log_custom(f"Found {len(links)} total links on this page")
        
        # Filter links to avoid duplication
        filtered_links = []
        for link in links:
            url = link.attrib['href']
            # Skip non-product URLs
            if any(skip in url for skip in ['/new', '/create', '/all']):
                continue
                
            full_url = response.urljoin(url)
            
            # Skip if we've already processed this URL in this session
            if full_url in self.processed_urls:
                continue
                
            # Mark URL as processed
            self.processed_urls.add(full_url)
            filtered_links.append(link)
        
        self.log_custom(f"Processing {len(filtered_links)} new product links after filtering")
        
        # Process each product link with enhanced cache utilization
        page_products = 0
        cached_products = 0
        matched_cached_products = 0
        
        for link in filtered_links:
            url = link.attrib['href']
            full_url = response.urljoin(url)
            page_products += 1
            
            # Check if product is in cache
            if self.use_cache and self.product_cache.has_product(full_url):
                cached_product = self.product_cache.get_product(full_url)
                cached_products += 1
                
                # OPTION 1: If product was already matched, we can skip requesting entirely
                if cached_product.get('matched', False):
                    matched_cached_products += 1
                    self.log_custom(f"Cache hit for matched product: {cached_product.get('full_name', full_url)}")
                    
                    # Add matched product directly to the batch from cache
                    self.product_batch.append(cached_product)
                    if len(self.product_batch) >= self.batch_size:
                        self.save_batch()
                        
                    continue
                
                # OPTION 2: For unmatched products, try matching again with the cached name
                else:
                    self.log_custom(f"Cache hit for unmatched product: {cached_product.get('full_name', full_url)}")
                    full_name = cached_product.get('full_name')
                    
                    if full_name:
                        # Try fuzzy matching with the cached name
                        is_match, matched_name = self.check_fuzzy_match(full_name)
                        
                        if is_match:
                            self.log_custom(f"Found new match for previously unmatched product: {full_name}")
                            # Update the cache entry
                            cached_product['matched'] = True
                            cached_product['matched_name'] = matched_name
                            cached_product['category'] = self.product_categories.get(matched_name, '') if hasattr(self, 'product_categories') else ''
                            
                            # Increment matched count and add to batch
                            self.matched_count += 1
                            self.product_batch.append(cached_product)
                            if len(self.product_batch) >= self.batch_size:
                                self.save_batch()
                            
                            # Update cache
                            self.product_cache.add_product(cached_product)
                            continue
                        else:
                            self.log_custom(f"Still no match for cached product: {full_name}")
                            # Skip fetching since we know it won't match
                            continue
            
            # OPTION 3: If not in cache or couldn't match from cache, request the product page
            yield scrapy.Request(
                full_url,
                callback=self.parse_product,
                priority=1,  # Higher priority for product pages
                meta={'product_url': full_url}
            )
        
        # Log cache statistics with enhanced metrics
        if self.use_cache:
            self.log_custom(f"Processing stats: {page_products} products on page, {cached_products} from cache ({matched_cached_products} matched)")
            cache_stats = self.product_cache.get_stats()
            self.log_custom(f"Cache stats: {cache_stats['size']} items, {cache_stats['hit_ratio']:.2%} hit ratio")
            
            # Estimate time saved by using cache
            time_saved_estimate = matched_cached_products * 2.5  # Assuming 2.5 seconds saved per matched product
            self.log_custom(f"Estimated time saved: {time_saved_estimate:.2f} seconds")
        else:
            self.log_custom(f"Found {page_products} products on this page")
        
        # Save checkpoint after processing each page
        self.save_checkpoint_safe(current_offset)
        
        # Handle pagination with offset increment by 1
        next_offset = current_offset + 1
        self.log_custom(f"Pagination check: page_count={self.page_count}, max_pages={self.max_pages}")

        # Continue if we haven't reached max pages
        if self.page_count < self.max_pages:
            # Construct next page URL with offset+1
            next_url = f'https://incidecoder.com/products/all?offset={next_offset}'
            
            self.log_custom(f"Using forced next page URL with calculated offset")
            self.log_custom(f"Current page: {current_offset + 1}, next page: {next_offset + 1}")
            self.log_custom(f"Next URL will be: {next_url}")
            
            yield scrapy.Request(next_url, self.parse, priority=0)
        else:
            self.log_custom(f"Reached max pages limit ({self.max_pages}), stopping pagination")
    
    def check_fuzzy_match(self, product_name):
        """Check if product name fuzzy matches with any Skinsort product"""
        if not self.skinsort_products:
            return False, None
            
        try:
            # Use RapidFuzz for fast fuzzy matching
            match = process.extractOne(product_name, self.skinsort_products)
            
            if match:
                self.log_custom(f"Best match for '{product_name}': '{match[0]}' with score {match[1]}")
                
                # Stricter matching for shorter names
                name_length = len(product_name)
                required_threshold = self.similarity_threshold
                
                if name_length < 15:
                    required_threshold = max(90, self.similarity_threshold)
                
                if match[1] >= required_threshold:
                    return True, match[0]
            
            return False, None
            
        except Exception as e:
            self.log_custom(f"Error in fuzzy matching: {str(e)}", logging.ERROR)
            return False, None
    
    def parse_product(self, response):
        """Parse individual product pages with enhanced caching logic"""
        self.product_count += 1
        product_url = response.url
        
        try:
            # Extract minimal data first for matching
            product_name = response.css('#product-title::text').get('').strip()
            brand = response.css('#product-brand-title a::text').get('').strip()
            full_name = f"{brand} {product_name}" if brand else product_name
            
            # Check for match before proceeding with full extraction
            is_match, matched_name = self.check_fuzzy_match(full_name)
            
            # Create a basic record for all products
            basic_product = {
                'name': product_name,
                'brand': brand,
                'full_name': full_name,
                'url': product_url,
                'matched': is_match,
                'page_found': self.page_count,
                'scrape_date': datetime.now().isoformat()
            }
            
            # Add category if we have a match
            if is_match and matched_name and hasattr(self, 'product_categories'):
                basic_product['category'] = self.product_categories.get(matched_name, '')
            
            # Skip detailed extraction for non-matching products
            if not is_match:
                self.log_custom(f"\nSkipping detailed parsing for non-matched product {self.product_count}: {full_name}")
                
                # Add to cache even if not a match
                if self.use_cache:
                    self.product_cache.add_product(basic_product)
                    
                yield basic_product
                return
            
            # For matched products, continue with full extraction
            self.matched_count += 1
            self.log_custom(f"\nProcessing matched product {self.matched_count}/{self.product_count}: {product_url}")
            
            # Build detailed product data
            detailed_product = basic_product.copy()
            detailed_product.update({
                'matched_name': matched_name,
                'description': self.extract_description(response),
                'image_url': response.css('#product-main-image img::attr(src)').get(),
                'upload_info': self.extract_upload_info(response),
            })
            
            # Extract ingredients overview
            ingredients_overview = []
            ingredient_links = {}
            for ingredient in response.css('#ingredlist-short [role="listitem"]'):
                ing_name = ingredient.css('a.ingred-link::text').get('').strip()
                ing_link = ingredient.css('a.ingred-link::attr(href)').get()
                if ing_name and ing_link:
                    ingredients_overview.append(ing_name)
                    ingredient_links[ing_name] = response.urljoin(ing_link)
            
            detailed_product['ingredients_overview'] = ingredients_overview
            detailed_product['ingredient_links'] = ingredient_links
            
            # Extract product hashtags and key ingredients by function
            highlights = {
                'hashtags': [tag.strip('#') for tag in response.css('div.hashtags span.hashtag::text').getall()],
                'key_ingredients': {}
            }
            
            for func_block in response.css('div.ingredlist-by-function-block div'):
                function = func_block.css('span.bold a::text').get()
                if function:
                    ingredients = func_block.css('a.ingred-link::text').getall()
                    highlights['key_ingredients'][function.strip()] = [ing.strip() for ing in ingredients]
            
            detailed_product['highlights'] = highlights
            
            # Extract detailed ingredient information
            skim_through = []
            for row in response.css('table.product-skim tr'):
                if row.css('th'):
                    continue
                
                irritancy, comedogenicity = self.parse_irritancy_comedogenicity(row.css('td:nth-child(3)'))
                
                ingredient_data = {
                    'name': row.css('td:nth-child(1) a::text').get('').strip(),
                    'functions': [f.strip() for f in row.css('td:nth-child(2) a::text').getall()],
                    'irritancy': irritancy,
                    'comedogenicity': comedogenicity,
                    'rating': row.css('td:nth-child(4) span.our-take::text').get('')
                }
                
                if ingredient_data['name']:
                    skim_through.append(ingredient_data)
            
            detailed_product['skim_through'] = skim_through
            
            # Add to memory cache for future requests
            if self.use_cache:
                self.product_cache.add_product(detailed_product)
            
            # Add to batch and save if batch size reached
            self.product_batch.append(detailed_product)
            if len(self.product_batch) >= self.batch_size:
                self.save_batch()
            
            self.log_custom(f"Successfully processed matched product: {detailed_product['name']}")
            self.log_custom(f"Found {len(ingredients_overview)} ingredients")
            
            yield detailed_product
        
        except Exception as e:
            self.log_custom(f"Error processing {product_url}: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)
    
    def extract_description(self, response):
        """Extract full description including hidden parts"""
        desc_parts = []
        
        # Try product description div first
        product_desc = response.css('div.product-description').get()
        if product_desc:
            desc_parts.extend(response.css('div.product-description ::text').getall())
        
        # If empty, try showmore sections
        if not desc_parts:
            desc_parts.extend(response.css('#product-details span.showmore-section::text').getall())
            desc_parts.extend(response.css('#product-details span.showmore-desktop::text').getall())
        
        # If still empty, try the main product details
        if not desc_parts:
            desc_parts.extend(response.css('#product-details::text').getall())
        
        # Clean up and join
        description = ' '.join(part.strip() for part in desc_parts if part.strip())
        description = description.replace('\n', ' ').replace('\r', ' ')
        description = ' '.join(description.split())  # Remove extra spaces
        
        return description
    
    def extract_upload_info(self, response):
        """Extract and format upload information including date"""
        uploader = response.css('div.fs12::text').get('')
        date = response.css('div.fs12 time::text').get('')
        if uploader and date:
            return f"{uploader.strip()} {date.strip()}"
        return None
    
    def parse_irritancy_comedogenicity(self, cell):
        """Parse irritancy and comedogenicity ratings from table cell"""
        spans = cell.css('span.irrncom span[class*="colorcode"]')
        if not spans:
            return None, None
            
        # Split spans into two groups (first half for irritancy, second half for comedogenicity)
        irritancy_spans = spans[:len(spans)//2]
        comedogenicity_spans = spans[len(spans)//2:]
        
        def process_values(value_spans):
            """Process a list of spans into a single value or range"""
            values = [span.css('::text').get('').strip() for span in value_spans]
            if not values:
                return None
            if len(values) == 1:
                return values[0]
            if len(values) == 2:
                return values[0] if values[0] == values[1] else f"{values[0]}-{values[1]}"
            return None
            
        return process_values(irritancy_spans), process_values(comedogenicity_spans)
    
    def closed(self, reason):
        """Generate detailed report when spider closes, including cache statistics"""
        # Save any remaining products in the last batch
        if self.product_batch:
            self.save_batch()
            
        # Save final checkpoint
        self.save_checkpoint_safe()
        
        # Create a summary report
        self.log_custom(f"\n{'='*50}")
        self.log_custom(f"Crawl finished! Reason: {reason}")
        self.log_custom(f"Total pages processed (internal counter): {self.page_count}")
        self.log_custom(f"Total products found: {self.product_count}")
        self.log_custom(f"Total matched products: {self.matched_count}")
        
        match_percentage = (self.matched_count/self.product_count)*100 if self.product_count > 0 else 0
        self.log_custom(f"Match percentage: {match_percentage:.2f}%")
        self.log_custom(f"Total unique URLs processed: {len(self.processed_urls)}")
        
        # Log cache statistics if cache was used
        if self.use_cache:
            cache_stats = self.product_cache.get_stats()
            self.log_custom(f"\nCache Statistics:")
            self.log_custom(f"Total size: {cache_stats['size']} products")
            self.log_custom(f"Cache hits: {cache_stats['hits']}")
            self.log_custom(f"Cache misses: {cache_stats['misses']}")
            self.log_custom(f"Hit ratio: {cache_stats['hit_ratio']:.2%}")
            
            memory_saved = cache_stats['hits'] * 8  # Rough estimate: 8KB per request saved
            self.log_custom(f"Estimated memory saved: {memory_saved/1024:.2f} MB")
            
            time_saved = cache_stats['hits'] * 1.5  # Rough estimate: 1.5 seconds per request saved
            self.log_custom(f"Estimated time saved: {time_saved:.2f} seconds")
        
        # Add diagnosis information
        avg_products_per_page = self.product_count / self.page_count if self.page_count > 0 else 0
        self.log_custom(f"Average products per page: {avg_products_per_page:.2f}")
        
        # Save crawler summary
        summary = {
            'total_pages': self.page_count,
            'total_products': self.product_count,
            'matched_products': self.matched_count,
            'match_percentage': match_percentage,
            'total_batches': self.batch_count,
            'crawl_date': datetime.now().isoformat(),
            'reason_finished': reason,
            'similarity_threshold': self.similarity_threshold
        }
        
        # Add cache stats to summary if cache was used
        if self.use_cache:
            summary['cache_stats'] = cache_stats
        
        try:
            with open(os.path.join(self.output_dir, 'crawl_summary.json'), 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
                
            self.log_custom(f"Saved crawl summary to {self.output_dir}/crawl_summary.json")
        except Exception as e:
            self.log_custom(f"Error saving crawl summary: {str(e)}", logging.ERROR)
            
        self.log_custom('='*50)


class CrawlerManager:
    """Manages the execution of the spider with pauses between batches"""
    
    def __init__(self, config=None):
        """Initialize the manager with configuration"""
        self.config = config or CONFIG.copy()
        #self.setup_logging()
        self.setup_signal_handlers()
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.config["OUTPUT_DIR"]):
            os.makedirs(self.config["OUTPUT_DIR"])
            
        # Use the global manager logger
        global LOGGERS  # Use global keyword to access the global variable
        self.logger = LOGGERS['manager'] if LOGGERS else logging.getLogger()

    def setup_signal_handlers(self):
        """Set up handlers for system signals"""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
        # Windows doesn't support SIGUSR1
        if hasattr(signal, 'SIGUSR1'):
            signal.signal(signal.SIGUSR1, self.handle_pause)
    
    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received shutdown signal ({signum}). Cleaning up...")
        self.logger.info("Shutdown complete. Exiting.")
        sys.exit(0)
    
    def handle_pause(self, signum, frame):
        """Handle pause signal"""
        self.logger.info(f"Received pause signal ({signum}). Pausing...")
        time.sleep(60)  # Pause for 1 minute
        self.logger.info("Resuming after pause")
    
    def check_system_resources(self):
        """Check if system has enough resources to continue"""
        if not self.config["RESOURCE_CHECK_ENABLED"]:
            return True
            
        try:
            import psutil
            
            # Get memory info
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Get disk info for the current directory
            disk = psutil.disk_usage('.')
            disk_percent = disk.percent
            
            self.logger.info(f"System status: Memory usage {memory_percent}%, Disk usage {disk_percent}%")
            
            # Pause if resources are critically low
            if memory_percent > self.config["MAX_MEMORY_PERCENT"] or disk_percent > self.config["MAX_DISK_PERCENT"]:
                self.logger.warning(f"System resources critically low, pausing for 30 minutes")
                time.sleep(1800)  # 30 minutes
                return False
            
            return True
        except ImportError:
            self.logger.warning("psutil not installed. Resource monitoring disabled.")
            return True
        except Exception as e:
            self.logger.error(f"Error checking system resources: {str(e)}")
            return True  # Continue even if resource check fails
    
    def get_current_page(self):
        """Get the next offset to start from based on checkpoint file"""
        checkpoint_file = self.config["CHECKPOINT_FILE"]
        if not os.path.exists(checkpoint_file):
            self.logger.info(f"No checkpoint file found at {checkpoint_file}")
            return 0
                    
        try:
            with open(checkpoint_file, "rb") as f:
                self.logger.info(f"Reading checkpoint file from {checkpoint_file}")
                checkpoint = pickle.load(f)
                
                # Get last_offset with proper handling of None and invalid values
                last_offset = checkpoint.get('last_offset')
                
                # Validate the offset value
                if last_offset is None:
                    self.logger.warning("Found None value for last_offset in checkpoint, defaulting to 0")
                    last_offset = 0
                elif not isinstance(last_offset, int):
                    try:
                        last_offset = int(last_offset)
                    except (ValueError, TypeError):
                        self.logger.warning(f"Invalid last_offset value: {last_offset}, defaulting to 0")
                        last_offset = 0
                        
                # Log the page count from the checkpoint
                page_count = checkpoint.get('page_count', 0)
                self.logger.info(f"Checkpoint shows {page_count} pages processed and last_offset={last_offset}")
                
                # We don't use this value for progression anymore, but return it for informational purposes
                return last_offset
        except Exception as e:
            self.logger.error(f"Error reading checkpoint: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return 0


    def run(self):
        """Run the crawler in batches with pauses in between - CORRECTED VERSION"""
        self.logger.info("Starting crawler manager")
        
        # For informational purposes only, get the current offset from checkpoint
        checkpoint_offset = self.get_current_page()
        self.logger.info(f"Checkpoint shows last processed offset: {checkpoint_offset}")
        
        # Start with offset 0 (page 1) or from command line argument if provided
        # We don't use the checkpoint offset for this anymore
        current_offset = 0
        
        # Calculate total pages
        total_pages = self.config["TOTAL_PAGES"]
        self.logger.info(f"Will process {total_pages} total pages")
        
        # Clear the checkpoint file to avoid confusion
        if os.path.exists(self.config["CHECKPOINT_FILE"]):
            self.logger.info(f"Creating a clean checkpoint to avoid confusion")
            try:
                # Create a clean checkpoint that won't interfere with our progression
                clean_checkpoint = {
                    'processed_urls': set(),
                    'product_count': 0,
                    'matched_count': 0,
                    'page_count': 0,
                    'batch_count': 0,
                    'last_offset': 0,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.config["CHECKPOINT_FILE"], 'wb') as f:
                    pickle.dump(clean_checkpoint, f)
            except Exception as e:
                self.logger.error(f"Error creating clean checkpoint: {str(e)}")
        
        # Track pages processed for proper progression
        total_pages_processed = 0
        
        while total_pages_processed < total_pages:
            # Check system resources before starting a new batch
            if not self.check_system_resources():
                self.logger.info("Delaying due to resource constraints")
                time.sleep(300)  # Wait 5 minutes before checking again
                continue
                    
            # Calculate pages for this batch
            pages_per_batch = self.config["PAGES_PER_BATCH"]
            
            # Make sure we don't exceed the total_pages limit
            actual_pages_this_batch = min(pages_per_batch, total_pages - total_pages_processed)
            
            # Calculate the current starting offset for this batch
            current_offset = total_pages_processed
            
            # Log the pages being processed
            self.logger.info(
                f"Processing batch starting at page {current_offset + 1} for {actual_pages_this_batch} pages"
            )
            self.logger.info(
                f"Will process pages {current_offset + 1} to {current_offset + actual_pages_this_batch}"
            )
            
            # CRITICAL FIX: Pass the current_offset to the spider for this batch
            success = self.run_spider_batch(current_offset, actual_pages_this_batch)
            
            if success:
                # Increment our count of processed pages
                total_pages_processed += actual_pages_this_batch
                
                # Log the next batch we'll process
                self.logger.info(f"Successfully processed {actual_pages_this_batch} pages")
                self.logger.info(f"Total pages processed: {total_pages_processed} of {total_pages}")
                
                if total_pages_processed < total_pages:
                    self.logger.info(f"Next batch will start at page {total_pages_processed + 1}")
            else:
                # If there was a failure, retry the same offset after a longer delay
                self.logger.info(f"Failed processing batch starting at page {current_offset + 1}, will retry")
                time.sleep(self.config["PAUSE_MINUTES"] * 120)  # Double the normal pause time
                continue
            
            # If we've reached the total pages, break
            if total_pages_processed >= total_pages:
                self.logger.info(f"Reached target of {total_pages} pages. Crawling complete.")
                break
                    
            # Pause before next batch
            pause_seconds = self.config["PAUSE_MINUTES"] * 60
            if pause_seconds > 0:
                self.logger.info(f"Pausing for {self.config['PAUSE_MINUTES']} minutes before next batch")
                time.sleep(pause_seconds)
        
        self.logger.info("Crawler manager finished successfully")


    def run_spider_batch(self, start_offset, pages_per_batch):
        """Run the spider for a specific batch of pages"""
        self.logger.info(f"Running spider for pages {start_offset + 1} to {start_offset + pages_per_batch}")
        self.logger.info(f"Using start offset: {start_offset}")
        
        # Initialize LOGGERS before running the spider
        global LOGGERS
        if not LOGGERS:
            LOGGERS = setup_logging(self.config["OUTPUT_DIR"])
        
        # Create a unique output directory for this batch
        batch_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        batch_output_dir = os.path.join(
            self.config["OUTPUT_DIR"], 
            f"batch_pages_{start_offset + 1}_to_{start_offset + pages_per_batch}_{batch_timestamp}"
        )
        
        # Ensure the batch directory exists
        if not os.path.exists(batch_output_dir):
            os.makedirs(batch_output_dir)
        
        # Create a batch-specific checkpoint file to avoid interference
        batch_checkpoint_file = os.path.join(batch_output_dir, "batch_checkpoint.pkl")
        
        # Initialize a clean checkpoint file with page_count=0
        clean_checkpoint = {
            'processed_urls': set(),
            'product_count': 0,
            'matched_count': 0,
            'page_count': 0,  # Reset page count to 0
            'batch_count': 0,
            'last_offset': start_offset,  # Start from the correct offset
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(batch_checkpoint_file, 'wb') as f:
                pickle.dump(clean_checkpoint, f)
            self.logger.info(f"Created fresh checkpoint file at {batch_checkpoint_file} with page_count=0")
        except Exception as e:
            self.logger.error(f"Error creating fresh checkpoint: {str(e)}")
        
        # Construct the scrapy command using the batch-specific checkpoint
        cmd = [
            "scrapy",
            "runspider",
            __file__,  # Use this file itself as the spider
            "-a", f"skinsort_json={self.config['SKINSORT_JSON']}",
            "-a", f"similarity_threshold={self.config['SIMILARITY_THRESHOLD']}",
            "-a", f"batch_size={self.config['BATCH_SIZE']}",
            "-a", f"start_page={start_offset}",  # Pass the exact offset value
            "-a", f"max_pages={pages_per_batch}",
            "-a", f"checkpoint_file={batch_checkpoint_file}",  # Use the batch-specific checkpoint
            "-a", f"output_dir={batch_output_dir}",
            "-a", f"cache_file={self.config.get('CACHE_FILE', 'product_cache.json')}",
        ]
        
        # Log the exact command
        self.logger.info(f"Spider command: {' '.join(cmd)}")
        
        # Run the spider as a subprocess
        try:
            self.logger.info(f"Starting spider subprocess...")
            process = subprocess.run(cmd, check=True)
            self.logger.info(f"Spider completed batch with exit code {process.returncode}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Spider failed with exit code {e.returncode}")
            return False

def parse_args():
    """Parse command line arguments"""
    import argparse
    import sys
    
    # Start with a copy of the global config
    config = CONFIG.copy()
    
    # If no arguments were provided, just return the config
    if len(sys.argv) <= 1:
        return config
    
    parser = argparse.ArgumentParser(description="Auto-resuming web scraper with checkpointing")
    
    # Arguments with CONFIG defaults
    parser.add_argument("--skinsort-json", default=CONFIG["SKINSORT_JSON"], 
                        help=f"Path to JSON file with reference products (default: {CONFIG['SKINSORT_JSON']})")
    
    parser.add_argument("--checkpoint-file", default=CONFIG["CHECKPOINT_FILE"], 
                        help=f"Path to checkpoint file (default: {CONFIG['CHECKPOINT_FILE']})")
    
    parser.add_argument("--pages-per-batch", type=int, default=CONFIG["PAGES_PER_BATCH"], 
                        help=f"Number of pages to crawl before pausing (default: {CONFIG['PAGES_PER_BATCH']})")
    
    parser.add_argument("--total-pages", type=int, default=CONFIG["TOTAL_PAGES"], 
                        help=f"Total number of pages to crawl (default: {CONFIG['TOTAL_PAGES']})")
    
    parser.add_argument("--pause-minutes", type=int, default=CONFIG["PAUSE_MINUTES"], 
                        help=f"Pause duration between batches (default: {CONFIG['PAUSE_MINUTES']})")
    
    parser.add_argument("--similarity-threshold", type=float, default=CONFIG["SIMILARITY_THRESHOLD"], 
                        help=f"Threshold for fuzzy matching (default: {CONFIG['SIMILARITY_THRESHOLD']})")
    
    parser.add_argument("--batch-size", type=int, default=CONFIG["BATCH_SIZE"], 
                        help=f"Number of products per output batch (default: {CONFIG['BATCH_SIZE']})")
    
    parser.add_argument("--output-dir", default=CONFIG["OUTPUT_DIR"], 
                        help="Directory for output files (default: auto-generated)")
    
    parser.add_argument("--no-resource-check", action="store_true", 
                        help="Disable system resource monitoring")
    
    args = parser.parse_args()
    
    # Update the config with parsed arguments
    config["SKINSORT_JSON"] = args.skinsort_json
    config["CHECKPOINT_FILE"] = args.checkpoint_file
    config["PAGES_PER_BATCH"] = args.pages_per_batch
    config["TOTAL_PAGES"] = args.total_pages
    config["PAUSE_MINUTES"] = args.pause_minutes
    config["SIMILARITY_THRESHOLD"] = args.similarity_threshold
    config["BATCH_SIZE"] = args.batch_size
    
    if args.output_dir:
        config["OUTPUT_DIR"] = args.output_dir
        
    if args.no_resource_check:
        config["RESOURCE_CHECK_ENABLED"] = False
    
    return config


def scrapy_crawl_entrypoint():
    """Entry point for Scrapy when run directly"""
    # Initialize the logging system with default output directory
    global LOGGERS
    LOGGERS = setup_logging()
    
    # Return the spider class for Scrapy to use
    return OptimizedProductSpider


def is_run_by_scrapy():
    """Check if this script is being run by Scrapy"""
    return any('scrapy' in arg.lower() for arg in sys.argv)

def main():
    """Main entry point when run as a script"""
    global LOGGERS
    
    try:
        # Parse command line arguments and update configuration
        config = parse_args()
        
        # Initialize logging system
        LOGGERS = setup_logging(config["OUTPUT_DIR"])
        LOGGERS['manager'].info("Starting crawler manager script")
        
        # Create and run the crawler manager
        manager = CrawlerManager(config)
        manager.run()
        
        LOGGERS['manager'].info("Crawler manager completed successfully")
        
    except KeyboardInterrupt:
        if LOGGERS:
            LOGGERS['manager'].info("Interrupted by user. Exiting gracefully.")
        else:
            print("Interrupted by user. Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        if LOGGERS:
            LOGGERS['manager'].critical(f"Unhandled exception: {str(e)}")
            LOGGERS['manager'].critical(f"Traceback: {traceback.format_exc()}")
        else:
            print(f"Unhandled exception: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    # Check if being run by Scrapy or directly
    if is_run_by_scrapy():
        # Do nothing special when run by Scrapy - the OptimizedProductSpider will be picked up
        pass
    else:
        # When run directly as a script, execute main function
        main()