"""
Optimized Product Spider Module for Khair Scraper

This module provides the main Scrapy spider for product data extraction
with automatic resumption and resource optimization.
"""

import os
import re
import json
import time
import pickle
import logging
import traceback
from datetime import datetime
import random

import scrapy
from rapidfuzz import fuzz, process

# Import local modules
from khair_scraper.utils.config import CONFIG, SPIDER_SETTINGS
from khair_scraper.middleware.retry_middleware import CustomRetryMiddleware
from json_file_cache import JsonFileCache  # Keep external dependency

class OptimizedProductSpider(scrapy.Spider):
    """
    Optimized Spider for scraping product information with checkpoint capabilities.
    
    This spider crawls product pages, extracts detailed information, 
    and matches products against a reference dataset.
    """
    
    name = 'optimized_products'
    start_urls = ['https://incidecoder.com/products/all']
    
    custom_settings = SPIDER_SETTINGS
    
    def __init__(self, skinsort_json=None, similarity_threshold=None, batch_size=None, 
                 start_page=None, max_pages=None, checkpoint_file=None, output_dir=None, 
                 use_cache=True, cache_file=None, *args, **kwargs):
        """
        Initialize the spider with configurable parameters and file-based cache.
        
        Args:
            skinsort_json (str, optional): Path to JSON file with reference products
            similarity_threshold (int, optional): Threshold for fuzzy matching (0-100)
            batch_size (int, optional): Number of products per output batch
            start_page (int, optional): Page to start scraping from
            max_pages (int, optional): Maximum pages to scrape
            checkpoint_file (str, optional): Path to checkpoint file for resuming
            output_dir (str, optional): Directory to store output files
            use_cache (bool, optional): Whether to use product caching
            cache_file (str, optional): Path to the cache file
        """
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
        self.page_count = 0  # Will be incremented in parse()
        self.batch_count = 0
        self.product_batch = []
        
        # Initialize the last_offset attribute for progress tracking
        self.last_offset = self.start_page
        
        # Load Skinsort data
        self.load_skinsort_data(self.skinsort_json)
        
        # Load run log file path from logger if available 
        self.run_log_file = getattr(self, 'run_log_file', None)
        self.timestamp_str = getattr(self, 'timestamp_str', None)
        if self.timestamp_str is None:
            self.timestamp_str = "No_timestamp"  

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
        """
        Load state from checkpoint file with improved handling.
        
        Returns:
            bool: True if checkpoint was loaded successfully, False otherwise
        """
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            self.log_custom("No checkpoint file found, starting from scratch", logging.INFO)
            return False
            
        try:
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint = pickle.load(f)
                
            # Log the checkpoint content without processed_urls and with variable descriptions
            self.log_custom(f"Loaded checkpoint: product_count={checkpoint.get('product_count', 0)}, "
                            f"matched_count={checkpoint.get('matched_count', 0)}, "
                            f"page_count={checkpoint.get('page_count', 0)}, "
                            f"batch_count={checkpoint.get('batch_count', 0)}, "
                            f"last_offset={checkpoint.get('last_offset', None)}")
            
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
            
            return True
            
        except Exception as e:
            self.log_custom(f"Error loading checkpoint: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)
            self.log_custom("Starting from beginning due to checkpoint error", logging.WARNING)
            return False
    
    def log_custom(self, message, level=logging.INFO):
        """
        Single logging method to use throughout the spider.
        
        Args:
            message (str): The message to log
            level (int, optional): Log level, default is INFO
        """
        # Log to Scrapy's logger
        self.logger.log(level, message)
        
        # Also log to our custom file if available
        try:
            if hasattr(self, 'run_log_file') and self.run_log_file:
                log_dir = os.path.dirname(self.run_log_file)
                # Make sure the directory exists
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                    
                # Append to log file
                with open(self.run_log_file, 'a', encoding='utf-8') as f:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    f.write(f"{timestamp} - {message}\n")
        except Exception as e:
            self.logger.error(f"Error writing to run log file: {str(e)}")
        
    def load_skinsort_data(self, skinsort_json):
        """
        Load product data from Skinsort JSON file.
        
        Args:
            skinsort_json (str): Path to the JSON file containing reference products
            
        Raises:
            ValueError: If the JSON file is invalid or missing required fields
        """
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
    
    def save_checkpoint_safe(self, current_offset=None):
        """
        Save crawler state with proper value persistence.
        
        Args:
            current_offset (int, optional): Current page offset to save
            
        Returns:
            bool: True if checkpoint was saved successfully, False otherwise
        """
        if not self.checkpoint_file:
            self.log_custom("No checkpoint file specified, skipping checkpoint")
            return False
        
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
            return True
        except Exception as e:
            self.log_custom(f"Error saving checkpoint: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)
            return False

    def save_batch(self):
        """
        Save current batch of products to disk and clear memory.
        
        Returns:
            bool: True if batch was saved successfully, False otherwise
        """
        if not self.product_batch:
            return False
            
        self.batch_count += 1
        filename = os.path.join(self.output_dir, f'product_batch_{self.batch_count}.json')
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.product_batch, f, indent=2, ensure_ascii=False)
                
            self.log_custom(f"Saved batch {self.batch_count} with {len(self.product_batch)} products to {filename}")
            # Clear memory after saving
            self.product_batch = []
            return True
        except Exception as e:
            self.log_custom(f"Error saving batch {self.batch_count}: {str(e)}", logging.ERROR)
            # Try to save to an alternative location
            alt_filename = os.path.join(self.output_dir, f'product_batch_{self.batch_count}_emergency.json')
            try:
                with open(alt_filename, 'w', encoding='utf-8') as f:
                    json.dump(self.product_batch, f, indent=2, ensure_ascii=False)
                self.log_custom(f"Saved emergency batch to {alt_filename}")
                self.product_batch = []
                return True
            except:
                self.log_custom(f"Failed to save batch {self.batch_count} to any location", logging.CRITICAL)
                return False

    def parse(self, response):
        """
        Parse the product listing page with improved cache utilization.
        
        Args:
            response: The HTTP response from the request
            
        Yields:
            scrapy.Request: New requests for product pages
        """
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
        self.log_custom(f"Output directory: {self.output_dir}")
        
        # Save checkpoint with the current offset
        self.save_checkpoint_safe(current_offset)
        
        # Find all product links on the page
        links = response.xpath('//a[contains(@href, "/products/")]')
        self.log_custom(f"Found {len(links)} total links on this page")
        
        # Debug - log the first few links to verify extraction
        for i, link in enumerate(links[:5]):
            self.log_custom(f"Debug - Link {i+1}: {link.attrib['href']}")
        
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

            # Increment product count for EVERY product URL found, regardless of cache status
            self.product_count += 1
            
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
        """
        Check if product name fuzzy matches with any Skinsort product.
        
        Args:
            product_name (str): The product name to match
            
        Returns:
            tuple: (is_match, matched_name) where is_match is a boolean and
                   matched_name is the name of the matched product or None
        """
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
        """
        Parse individual product pages with enhanced caching logic.
        
        Args:
            response: The HTTP response from the product page
            
        Returns:
            dict: Product data dictionary
        """
        product_url = response.meta.get('product_url', response.url)
        self.log_custom(f"Parsing product page: {product_url}")
        
        try:
            # Initialize an empty product_data dictionary
            product_data = {}
            
            # Extract product name and brand
            product_name = response.css('span#product-title::text').get()
            brand = response.css('span#product-brand-title a::text').get()
            
            # Process brand and name for full product name first (used for matching)
            if brand and product_name:
                brand = brand.strip()
                product_name = product_name.strip()
                full_name = f"{brand} {product_name}"
                
                # Set full_name as first field
                product_data['full_name'] = full_name
                
                # Set brand as second field
                product_data['brand'] = brand
                
                # Set name as third field
                product_data['name'] = product_name
                
                # Try fuzzy matching with Skinsort data
                is_match, matched_name = self.check_fuzzy_match(full_name)
            else:
                # Handle case where name or brand is missing
                if brand:
                    product_data['brand'] = brand.strip()
                if product_name:
                    product_data['name'] = product_name.strip()
                is_match = False
                matched_name = None
            
            # Extract product description (fourth field)
            product_description = response.css('span#product-details::text').get()
            if product_description:
                product_description = product_description.strip()
                product_data['description'] = product_description
            
            # Add URL (fifth field)
            product_data['url'] = product_url
            
            # Extract product image (sixth field)
            product_image = response.css('div.image img::attr(src)').get()
            if product_image:
                product_data['image_url'] = product_image
                self.log_custom(f"Found product image: {product_image}")
                
                # Try to get high resolution image if available
                if '@' in product_image:
                    # Extract base URL without the size modifier
                    base_img_url = product_image.split('@')[0]
                    # Get original/high-resolution version
                    product_data['high_res_image_url'] = f"{base_img_url}_original.jpeg"
                    self.log_custom(f"Created high-res image URL: {product_data['high_res_image_url']}")
            
            # Set matched status (seventh field)
            if 'full_name' in product_data:
                if is_match:
                    self.log_custom(f"Found match for: {product_data['full_name']} -> {matched_name}")
                    product_data['matched'] = True
                    product_data['matched_name'] = matched_name
                    product_data['category'] = self.product_categories.get(matched_name, '') if hasattr(self, 'product_categories') else ''
                    
                    # Increment matched count
                    self.matched_count += 1
                else:
                    product_data['matched'] = False
                    self.log_custom(f"No match found for: {product_data['full_name']}")
            else:
                product_data['matched'] = False

            # Extract ingredients table data (eighth field)
            ingredients_data = []

            # Find the ingredients table using the correct selector
            ingredients_table = response.css('div#ingredlist-table-section table.product-skim')
            
            if ingredients_table:
                self.log_custom("Found ingredients table")
                
                # Get all rows from the table body
                rows = ingredients_table.css('tbody tr')
                
                self.log_custom(f"Found {len(rows)} ingredient rows in table")
                
                # Filter out duplicate ingredients that might appear in both mobile and desktop versions
                processed_ingredients = set()
                
                for row in rows:
                    ingredient = {}
                    
                    # Extract ingredient name and link
                    name_cell = row.css('td:nth-child(1)')
                    ingredient_link = name_cell.css('a::attr(href)').get()
                    name = name_cell.css('a::text').get()
                    
                    if name:
                        ingredient['name'] = name.strip()
                        
                    if ingredient_link:
                        ingredient['ingredient_link'] = response.urljoin(ingredient_link)
                    
                    # Extract what it does
                    what_it_does_cell = row.css('td:nth-child(2)')
                    what_it_does_links = what_it_does_cell.css('a::text').getall()
                    
                    if what_it_does_links:
                        ingredient['what_it_does'] = [wid.strip() for wid in what_it_does_links if wid.strip()]
                    
                    # Extract irritancy rating
                    irritancy_cell = row.css('td:nth-child(3)')
                    
                    # Get all spans with title attributes containing "irritancy" or "comedogenicity"
                    irritancy_spans = irritancy_cell.css('span[title*="irritancy"]')
                    comedogenicity_spans = irritancy_cell.css('span[title*="comedogenicity"]')
                    
                    # Track the multiple values we might find
                    irritancy_values = []
                    comedogenicity_values = []
                    
                    # Process irritancy values
                    if irritancy_spans:
                        for span in irritancy_spans:
                            title_attr = span.attrib.get('title', '')
                            
                            try:
                                # Format is 'irritancy: X' where X is the value
                                value = title_attr.split(':')[1].strip()
                                irritancy_values.append(value)
                            except:
                                self.log_custom("Error extracting irritancy value from title", logging.WARNING)
                        
                        # Store all values in the ingredient dictionary
                        if irritancy_values:
                            ingredient['irritancy_values'] = irritancy_values
                            
                    # Process comedogenicity values
                    if comedogenicity_spans:
                        for span in comedogenicity_spans:
                            title_attr = span.attrib.get('title', '')
                            
                            try:
                                # Format is 'comedogenicity: X' where X is the value
                                value = title_attr.split(':')[1].strip()
                                comedogenicity_values.append(value)
                            except:
                                self.log_custom("Error extracting comedogenicity value from title", logging.WARNING)
                        
                        # Store all values in the ingredient dictionary
                        if comedogenicity_values:
                            ingredient['comedogenicity_values'] = comedogenicity_values
                    
                    # Extract ID/Rating
                    id_rating_cell = row.css('td:nth-child(4)')
                    id_rating = id_rating_cell.css('span.our-take::text').get()
                    if id_rating:
                        ingredient['id_rating'] = id_rating.strip()
                    
                    # Check if this ingredient has a valid name and hasn't been processed yet
                    # This helps avoid duplicates from mobile/desktop version rows
                    if ingredient.get('name') and ingredient['name'] not in processed_ingredients:
                        ingredients_data.append(ingredient)
                        processed_ingredients.add(ingredient['name'])
                
                product_data['ingredients'] = ingredients_data
                self.log_custom(f"Extracted {len(ingredients_data)} unique ingredients")
            else:
                self.log_custom("No ingredients table found on this page")
                product_data['ingredients'] = []
            
            # Add timestamp after all the requested fields
            product_data['timestamp'] = datetime.now().isoformat()
            
            # Try to extract product highlights if available
            highlights = []
            highlight_section = response.css('div#ingredlist-highlights-section')
            if highlight_section:
                # Extract hashtags
                hashtags = highlight_section.css('span.hashtag::text').getall()
                if hashtags:
                    product_data['hashtags'] = [tag.strip() for tag in hashtags if tag.strip()]
                    
                # Extract key ingredients by function
                key_functions = highlight_section.css('div.ingredlist-by-function-block div')
                for func_div in key_functions:
                    func_name = func_div.css('span.bold a::text').get()
                    if func_name:
                        func_name = func_name.strip()
                        ingred_names = func_div.css('span:not(.bold) a::text').getall()
                        if ingred_names:
                            ingred_names = [name.strip() for name in ingred_names if name.strip()]
                            highlights.append({
                                'function': func_name,
                                'ingredients': ingred_names
                            })
                
                if highlights:
                    product_data['highlights'] = highlights
            
            # Add to product batch
            self.product_batch.append(product_data)
            
            # Save to cache if enabled
            if self.use_cache:
                self.product_cache.add_product(product_data)
            
            # Save batch if it reaches the specified size
            if len(self.product_batch) >= self.batch_size:
                self.save_batch()
            
            return product_data
            
        except Exception as e:
            self.log_custom(f"Error parsing product page: {str(e)}", logging.ERROR)
            self.log_custom(f"Traceback: {traceback.format_exc()}", logging.ERROR)
            
            # Create minimal product data for error case
            error_data = {
                'url': product_url,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'matched': False
            }
            
            # Still add to batch to track errors
            self.product_batch.append(error_data)
            
            # Save to cache if enabled
            if self.use_cache:
                self.product_cache.add_product(error_data)
                
            return error_data

    def closed(self, reason):
        """
        Handle spider closure with proper cleanup.
        
        Args:
            reason (str): Reason for closing the spider
        """
        self.log_custom(f"Spider closed: {reason}")
        
        # Save any pending product batch
        if self.product_batch:
            self.save_batch()
            
        # Final checkpoint save
        self.save_checkpoint_safe()
        
        # Log final stats
        self.log_custom(f"Final statistics: {self.page_count} pages, {self.product_count} products, {self.matched_count} matched")
        match_ratio = 0
        if self.product_count > 0:
            match_ratio = (self.matched_count / self.product_count) * 100
        self.log_custom(f"Match ratio: {match_ratio:.2f}%")
        
        # Save summary
        summary_file = os.path.join(self.output_dir, 'crawl_summary.json')
        try:
            summary = {
                'total_pages': self.page_count,
                'total_products': self.product_count,
                'matched_products': self.matched_count,
                'match_ratio': match_ratio,
                'start_time': getattr(self, 'start_time', None),
                'end_time': datetime.now().isoformat(),
                'reason': reason
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
                
            self.log_custom(f"Saved crawl summary to {summary_file}")
        except Exception as e:
            self.log_custom(f"Error saving crawl summary: {str(e)}", logging.ERROR) 