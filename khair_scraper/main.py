"""
Main Entry Point for Khair Scraper

This module provides the main entry point for running the scraper
either directly or via the Scrapy command-line interface.
"""

import os
import sys
import argparse
from datetime import datetime

# Import local modules
from khair_scraper.utils.config import CONFIG
from khair_scraper.utils.logging_setup import setup_logging
from khair_scraper.crawler_manager import CrawlerManager
from khair_scraper.spiders.optimized_product_spider import OptimizedProductSpider

def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Run the product scraper with batching and automatic resumption')
    
    parser.add_argument('--skinsort-json', dest='skinsort_json', type=str,
                        help=f'Skinsort products JSON file (default: {CONFIG["SKINSORT_JSON"]})')
                        
    parser.add_argument('--similarity-threshold', dest='similarity_threshold', type=int,
                        help=f'Fuzzy matching threshold (0-100, default: {CONFIG["SIMILARITY_THRESHOLD"]})')
                        
    parser.add_argument('--batch-size', dest='batch_size', type=int,
                        help=f'Products per output batch (default: {CONFIG["BATCH_SIZE"]})')
                        
    parser.add_argument('--start-page', dest='start_page', type=int,
                        help='Start from this page offset (0-based, default: auto-detect from checkpoint)')
                        
    parser.add_argument('--max-pages', dest='max_pages', type=int,
                        help=f'Maximum pages to crawl (default: {CONFIG["TOTAL_PAGES"]})')
                        
    parser.add_argument('--pages-per-batch', dest='pages_per_batch', type=int,
                        help=f'Pages per batch (default: {CONFIG["PAGES_PER_BATCH"]})')
                        
    parser.add_argument('--pause-minutes', dest='pause_minutes', type=int,
                        help=f'Minutes to pause between batches (default: {CONFIG["PAUSE_MINUTES"]})')
                        
    parser.add_argument('--checkpoint-file', dest='checkpoint_file', type=str,
                        help=f'Checkpoint file (default: {CONFIG["CHECKPOINT_FILE"]})')
                        
    parser.add_argument('--output-dir', dest='output_dir', type=str,
                        help=f'Output directory (default: {CONFIG["OUTPUT_DIR"]})')
                        
    parser.add_argument('--cache-file', dest='cache_file', type=str,
                        help=f'Cache file (default: {CONFIG["CACHE_FILE"]})')
                        
    parser.add_argument('--no-cache', dest='no_cache', action='store_true',
                        help='Disable caching (default: caching enabled)')
                        
    parser.add_argument('--no-resource-check', dest='no_resource_check', action='store_true',
                        help='Disable resource monitoring (default: monitoring enabled)')
                        
    args = parser.parse_args()
    return args

def update_config_from_args(args):
    """
    Update configuration with command line arguments.
    
    Args:
        args (argparse.Namespace): Parsed command line arguments
        
    Returns:
        dict: Updated configuration dictionary
    """
    # Make a copy of the default config
    config = CONFIG.copy()
    
    # Update config with args if provided
    if args.skinsort_json:
        config["SKINSORT_JSON"] = args.skinsort_json
        
    if args.similarity_threshold:
        config["SIMILARITY_THRESHOLD"] = args.similarity_threshold
        
    if args.batch_size:
        config["BATCH_SIZE"] = args.batch_size
        
    if args.max_pages:
        config["TOTAL_PAGES"] = args.max_pages
        
    if args.pages_per_batch:
        config["PAGES_PER_BATCH"] = args.pages_per_batch
        
    if args.pause_minutes is not None:  # Allow 0
        config["PAUSE_MINUTES"] = args.pause_minutes
        
    if args.checkpoint_file:
        config["CHECKPOINT_FILE"] = args.checkpoint_file
        
    if args.output_dir:
        config["OUTPUT_DIR"] = args.output_dir
        
    if args.cache_file:
        config["CACHE_FILE"] = args.cache_file
        
    if args.no_cache:
        config["USE_CACHE"] = False
        
    if args.no_resource_check:
        config["RESOURCE_CHECK_ENABLED"] = False
        
    # Special handling for start_page - we only override if explicitly provided
    if args.start_page is not None:
        config["START_PAGE"] = args.start_page
    
    return config

def is_run_by_scrapy():
    """
    Check if the script is being run directly by Scrapy.
    
    Returns:
        bool: True if being run by Scrapy, False otherwise
    """
    return "scrapy" in sys.argv[0]

def scrapy_crawl_entrypoint():
    """Entrypoint when run through the Scrapy command-line interface."""
    # Set up logging
    loggers = setup_logging()
    logger = loggers["manager"] if "manager" in loggers else None
    if logger:
        logger.info("Running through Scrapy CLI")
    
    # When run through Scrapy, we just return the spider class
    return OptimizedProductSpider

def main():
    """Main entry point for the scraper."""
    try:
        # Handle different execution methods
        if is_run_by_scrapy():
            return scrapy_crawl_entrypoint()
        
        # Parse command line arguments
        args = parse_args()
        
        # Update config with command line arguments
        config = update_config_from_args(args)
        
        # Print configuration for debugging
        print("\n" + "="*60)
        print("Starting Khair Product Scraper with configuration:")
        for key, value in config.items():
            print(f"  {key}: {value}")
        print("="*60 + "\n")
        
        # Make sure the output directory exists
        if not os.path.exists(config["OUTPUT_DIR"]):
            os.makedirs(config["OUTPUT_DIR"])
            print(f"Created output directory: {config['OUTPUT_DIR']}")
        
        # Create crawler manager with the configuration
        manager = CrawlerManager(config)
        
        print("Starting crawler manager...")
        # Run the crawler
        success = manager.run()
        
        # Exit with appropriate status code
        if success:
            print("\nScraper completed successfully!")
            sys.exit(0) 
        else:
            print("\nScraper did not complete successfully.")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unhandled exception in main: {str(e)}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    main() 