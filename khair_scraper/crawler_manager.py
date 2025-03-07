"""
Crawler Manager Module for Khair Scraper

This module provides a wrapper for running the spider with automatic
resumption, checkpointing, and resource monitoring.
"""

import os
import sys
import time
import signal
import logging
import argparse
import subprocess
from datetime import datetime
import json

import psutil
from scrapy import cmdline

# Import local modules
from khair_scraper.utils.config import CONFIG
from khair_scraper.utils.logging_setup import setup_logging
from khair_scraper.utils.stats_tracker import GlobalStatsTracker

class CrawlerManager:
    """
    Manager for running the spider with automatic resumption and resource monitoring.
    
    This class manages the execution of the spider, handling checkpoints,
    batching, and system resource monitoring.
    """
    
    def __init__(self, config=None):
        """
        Initialize the crawler manager.
        
        Args:
            config (dict, optional): Configuration dictionary, defaults to global CONFIG
        """
        # Use provided config or fall back to global config
        self.config = config or CONFIG
        
        # Initialize shutdown flag
        self.shutdown_requested = False
        self.pause_requested = False
        
        # Set up logging - FIRST to avoid AttributeError
        self.loggers = setup_logging(self.config["OUTPUT_DIR"])
        self.manager_logger = self.loggers["manager"]
        
        # Set up signal handlers for graceful shutdown - AFTER logging setup
        self.setup_signal_handlers()
        
        # Initialize stats tracker
        self.stats_tracker = GlobalStatsTracker(self.config["OUTPUT_DIR"])
        
        self.manager_logger.info("Crawler manager initialized")
    
    def setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown and pausing."""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
        # On Unix systems, we can also handle SIGUSR1 for pause/resume
        if hasattr(signal, 'SIGUSR1'):
            signal.signal(signal.SIGUSR1, self.handle_pause)
        
        self.manager_logger.info("Signal handlers set up")
    
    def handle_shutdown(self, signum, frame):
        """
        Handle shutdown signals.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        if self.shutdown_requested:
            self.manager_logger.warning("Forced shutdown requested, exiting immediately")
            sys.exit(1)
        
        self.manager_logger.info("Shutdown signal received, will stop after current batch")
        self.shutdown_requested = True
    
    def handle_pause(self, signum, frame):
        """
        Toggle pause state.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        self.pause_requested = not self.pause_requested
        status = "paused" if self.pause_requested else "resumed"
        self.manager_logger.info(f"Crawler {status} via signal")
    
    def check_system_resources(self):
        """
        Check if system resources are overloaded.
        
        Returns:
            tuple: (should_pause, reason) where should_pause is a boolean and reason is a string
        """
        if not self.config["RESOURCE_CHECK_ENABLED"]:
            return False, None
            
        try:
            # Check memory usage
            memory = psutil.virtual_memory()
            if memory.percent > self.config["MAX_MEMORY_PERCENT"]:
                return True, f"Memory usage too high: {memory.percent}% > {self.config['MAX_MEMORY_PERCENT']}%"
                
            # Check disk usage for output directory
            disk = psutil.disk_usage(self.config["OUTPUT_DIR"])
            if disk.percent > self.config["MAX_DISK_PERCENT"]:
                return True, f"Disk usage too high: {disk.percent}% > {self.config['MAX_DISK_PERCENT']}%"
                
            return False, None
        except Exception as e:
            self.manager_logger.error(f"Error checking system resources: {str(e)}")
            # Don't pause on error, just continue
            return False, None
    
    def get_current_page(self):
        """
        Determine the current page to start from based on checkpoint.
        
        Returns:
            int: Page offset to start from
        """
        checkpoint_file = self.config["CHECKPOINT_FILE"]
        start_page = 0
        
        if os.path.exists(checkpoint_file):
            try:
                import pickle
                with open(checkpoint_file, 'rb') as f:
                    checkpoint = pickle.load(f)
                
                if 'last_offset' in checkpoint:
                    # We want to start from the NEXT page after the last one processed
                    next_page = checkpoint['last_offset'] + 1
                    self.manager_logger.info(f"Resuming from checkpoint at page offset {next_page}")
                    return next_page
            except Exception as e:
                self.manager_logger.error(f"Error loading checkpoint to determine start page: {str(e)}")
                self.manager_logger.warning("Starting from the beginning")
        else:
            self.manager_logger.info("No checkpoint file found, starting from the beginning")
            
        return start_page
    
    def run(self):
        """
        Run the spider in batches with pauses between them.
        
        Returns:
            bool: True if completed successfully, False otherwise
        """
        self.manager_logger.info(f"Starting crawler with configuration: {self.config}")
        
        start_time = datetime.now()
        self.manager_logger.info(f"Start time: {start_time.isoformat()}")
        
        # Determine total number of pages and batch size
        total_pages = self.config["TOTAL_PAGES"]
        pages_per_batch = self.config["PAGES_PER_BATCH"]
        
        # Get current progress from checkpoint or start from beginning
        current_offset = self.get_current_page()
        self.manager_logger.info(f"Starting from page offset {current_offset}")
        
        # Calculate how many batches we need to run
        remaining_pages = max(0, total_pages - current_offset)
        total_batches = (remaining_pages + pages_per_batch - 1) // pages_per_batch
        
        # If we're resuming and already at or past the end, log and exit
        if current_offset >= total_pages:
            self.manager_logger.warning(f"Current offset {current_offset} >= total pages {total_pages}")
            self.manager_logger.warning("Nothing to do, already completed")
            return True
            
        self.manager_logger.info(f"Will run {total_batches} batches of {pages_per_batch} pages each")
        self.manager_logger.info(f"Total remaining pages: {remaining_pages} out of {total_pages}")
        
        batch_number = 1
        completed_successfully = False
        
        while current_offset < total_pages and not self.shutdown_requested:
            # Check if pause requested via signal
            if self.pause_requested:
                self.manager_logger.info("Crawler paused, waiting for resume signal")
                while self.pause_requested and not self.shutdown_requested:
                    time.sleep(1)
                if self.shutdown_requested:
                    break
                self.manager_logger.info("Crawler resumed")
            
            # Check system resources before starting a batch
            should_pause, reason = self.check_system_resources()
            if should_pause:
                self.manager_logger.warning(f"Pausing due to resource constraints: {reason}")
                # Sleep for a minute and check again
                time.sleep(60)
                continue
            
            # Calculate end page for this batch
            pages_this_batch = min(pages_per_batch, total_pages - current_offset)
            end_offset = current_offset + pages_this_batch - 1
            
            # Log batch information
            self.manager_logger.info(f"\n{'='*40}")
            self.manager_logger.info(f"Running batch {batch_number}/{total_batches}")
            self.manager_logger.info(f"Page range: {current_offset}-{end_offset} (inclusive)")
            
            # Create a timestamped batch directory
            batch_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            batch_dir = os.path.join(self.config["OUTPUT_DIR"], f"batch_{batch_number}_{batch_timestamp}")
            if not os.path.exists(batch_dir):
                os.makedirs(batch_dir)
            
            # Run the spider for this batch
            batch_success = self.run_spider_batch(batch_number, current_offset, pages_this_batch, batch_dir)
            
            if batch_success:
                self.manager_logger.info(f"Batch {batch_number} completed successfully")
                
                # Update global stats with results from this batch
                self.stats_tracker.update_from_batch(batch_dir)
                
                # Move to next batch
                current_offset += pages_this_batch
                batch_number += 1
                
                # If we have a pause time between batches, sleep
                if self.config["PAUSE_MINUTES"] > 0 and current_offset < total_pages:
                    pause_seconds = self.config["PAUSE_MINUTES"] * 60
                    self.manager_logger.info(f"Pausing for {self.config['PAUSE_MINUTES']} minutes between batches")
                    
                    # Sleep in small increments so we can check for shutdown signal
                    for _ in range(pause_seconds):
                        if self.shutdown_requested:
                            break
                        time.sleep(1)
            else:
                self.manager_logger.error(f"Batch {batch_number} failed, retrying in 30 seconds")
                time.sleep(30)
        
        # After all batches, generate final summary
        if current_offset >= total_pages:
            self.manager_logger.info("All pages processed, generating final summary")
            summary = self.stats_tracker.generate_final_summary()
            completed_successfully = True
        else:
            self.manager_logger.info("Spider stopped before completion")
            completed_successfully = False
        
        end_time = datetime.now()
        duration = end_time - start_time
        self.manager_logger.info(f"Crawler finished at {end_time.isoformat()}")
        self.manager_logger.info(f"Total duration: {duration}")
        
        return completed_successfully
    
    def run_spider_batch(self, batch_number, start_offset, pages_per_batch, batch_dir):
        """
        Run the spider for a batch of pages using subprocess to avoid reactor issues.
        
        Args:
            batch_number (int): Current batch number
            start_offset (int): Starting page offset
            pages_per_batch (int): Number of pages to process in this batch
            batch_dir (str): Batch directory path
            
        Returns:
            bool: True if batch completed successfully, False otherwise
        """
        try:
            # Create a batch-specific log file
            batch_log_file = os.path.join(batch_dir, f"batch_{batch_number}.log")
            self.manager_logger.info(f"Running spider from offset {start_offset} for {pages_per_batch} pages")
            self.manager_logger.info(f"Batch {batch_number} logs will be in: {batch_log_file}")
            
            # Create a batch-specific checkpoint file to avoid interference
            batch_checkpoint_file = os.path.join(batch_dir, "batch_checkpoint.pkl")
            
            # Initialize a clean checkpoint file with page_count=0
            import pickle
            from datetime import datetime
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
                self.manager_logger.info(f"Created fresh checkpoint file at {batch_checkpoint_file} with page_count=0")
            except Exception as e:
                self.manager_logger.error(f"Error creating fresh checkpoint: {str(e)}")
            
            # Set up environment variables for the subprocess
            env = os.environ.copy()
            
            # Build the command to run the spider in a separate process
            cmd = [
                sys.executable, "-m", "scrapy", "runspider", 
                "khair_scraper/spiders/optimized_product_spider.py",
                "-a", f"skinsort_json={self.config['SKINSORT_JSON']}",
                "-a", f"similarity_threshold={self.config['SIMILARITY_THRESHOLD']}",
                "-a", f"batch_size={self.config['BATCH_SIZE']}",
                "-a", f"start_page={start_offset}",
                "-a", f"max_pages={pages_per_batch}",
                "-a", f"checkpoint_file={batch_checkpoint_file}",  # Use the batch-specific checkpoint
                "-a", f"output_dir={batch_dir}",
                "-a", f"use_cache=True",
                "-a", f"cache_file={self.config['CACHE_FILE']}",
                "-s", "LOG_FILE=" + batch_log_file,
                "-s", "LOG_LEVEL=INFO",
                "-s", "DOWNLOAD_DELAY=2.0"
            ]
            
            self.manager_logger.info(f"Running command: {' '.join(cmd)}")
            
            # Run the spider in a subprocess to avoid reactor issues
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                env=env
            )
            
            # Wait for the process to complete
            stdout, stderr = process.communicate()
            
            # Check if process was successful
            if process.returncode == 0:
                self.manager_logger.info(f"Subprocess completed successfully for batch {batch_number}")
                
                # Create a batch summary
                self.create_batch_summary(batch_dir, start_offset, pages_per_batch, batch_number)
                
                return True
            else:
                self.manager_logger.error(f"Subprocess failed with code {process.returncode} for batch {batch_number}")
                self.manager_logger.error(f"Stderr: {stderr.decode('utf-8')}")
                return False
                
        except Exception as e:
            self.manager_logger.error(f"Error running spider batch: {str(e)}")
            import traceback
            self.manager_logger.error(traceback.format_exc())
            return False
            
    def create_batch_summary(self, batch_dir, start_offset, pages_per_batch, batch_number):
        """
        Create a batch summary file with statistics.
        
        Args:
            batch_dir (str): Batch directory path
            start_offset (int): Starting page offset
            pages_per_batch (int): Number of pages processed
            batch_number (int): Batch number
        """
        summary_file = os.path.join(batch_dir, 'crawl_summary.json')
        
        try:
            # Get product counts from the checkpoint file
            batch_checkpoint_file = os.path.join(batch_dir, "batch_checkpoint.pkl")
            total_products = 0
            matched_products = 0
            
            if os.path.exists(batch_checkpoint_file):
                try:
                    import pickle
                    with open(batch_checkpoint_file, 'rb') as f:
                        checkpoint = pickle.load(f)
                        total_products = checkpoint.get('product_count', 0)
                        matched_products = checkpoint.get('matched_count', 0)
                except Exception as e:
                    self.manager_logger.error(f"Error reading checkpoint file {batch_checkpoint_file}: {str(e)}")
            else:
                self.manager_logger.warning(f"Checkpoint file {batch_checkpoint_file} not found")
                
            # Create a summary with all available information
            summary = {
                'start_offset': start_offset,
                'pages_per_batch': pages_per_batch,
                'batch_number': batch_number,
                'total_products': total_products,
                'matched_products': matched_products,
                'end_offset': start_offset + pages_per_batch - 1,
                'timestamp': datetime.now().isoformat(),
                'batch_dir': batch_dir,
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
                
            self.manager_logger.info(f"Created batch summary at {summary_file}")
            self.manager_logger.info(f"Batch {batch_number} stats: {total_products} products, {matched_products} matched")
            
        except Exception as e:
            self.manager_logger.error(f"Error creating batch summary: {str(e)}")
            import traceback
            self.manager_logger.error(traceback.format_exc()) 