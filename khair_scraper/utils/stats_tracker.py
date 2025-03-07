"""
Stats Tracking Module for Khair Scraper

This module provides functionality for tracking and aggregating statistics
across multiple scraping batches.
"""

import os
import json
import logging
from datetime import datetime

from khair_scraper.utils.encoders import SetEncoder

class GlobalStatsTracker:
    """
    Class to track and aggregate statistics across multiple batches.
    
    This tracker maintains global statistics about the scraping process,
    including page counts, product counts, and match statistics.
    """
    
    def __init__(self, output_dir):
        """
        Initialize the stats tracker.
        
        Args:
            output_dir (str): Directory where stats files will be stored
        """
        self.output_dir = output_dir
        self.stats = {
            'total_pages': 0,
            'total_products': 0,
            'matched_products': 0,
            'total_batches': 0,
            'batch_summaries': []
        }
        self.stats_file = os.path.join(output_dir, 'global_stats.json')
        
        # Load existing stats if available
        self.load_stats()
    
    def load_stats(self):
        """
        Load statistics from file if it exists.
        
        Returns:
            bool: True if stats were loaded successfully, False otherwise
        """
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    saved_stats = json.load(f)
                
                # Restore all stats except processed_urls (which needs special handling)
                for key, value in saved_stats.items():
                    if key != 'processed_urls':
                        self.stats[key] = value
                
                logging.getLogger('manager').info(
                    f"Loaded global stats: {self.stats['total_pages']} pages, "
                    f"{self.stats['total_products']} products, "
                    f"{self.stats['matched_products']} matched"
                )
                return True
            except Exception as e:
                logging.getLogger('manager').error(f"Error loading global stats: {str(e)}")
                return False
        return False
    
    def update_from_batch(self, batch_dir):
        """
        Update global stats by reading a batch's summary file.
        
        Args:
            batch_dir (str): Directory containing the batch summary file
            
        Returns:
            bool: True if stats were updated successfully, False otherwise
        """
        summary_file = os.path.join(batch_dir, 'crawl_summary.json')
        if not os.path.exists(summary_file):
            logging.getLogger('manager').warning(f"No summary file found in {batch_dir}")
            return False
        
        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                batch_summary = json.load(f)
            
            # Update aggregated statistics
            self.stats['total_pages'] += batch_summary.get('total_pages', 0)
            self.stats['total_products'] += batch_summary.get('total_products', 0)
            self.stats['matched_products'] += batch_summary.get('matched_products', 0)
            self.stats['total_batches'] += 1
            
            # Add this batch summary to our list
            batch_summary['batch_dir'] = batch_dir
            self.stats['batch_summaries'].append(batch_summary)
            
            # Save updated stats
            self.save_stats()
            
            logging.getLogger('manager').info(
                f"Updated global stats with batch from {batch_dir}: "
                f"+{batch_summary.get('total_pages', 0)} pages, "
                f"+{batch_summary.get('total_products', 0)} products"
            )
            return True
        except Exception as e:
            logging.getLogger('manager').error(f"Error updating stats from batch {batch_dir}: {str(e)}")
            return False
    
    def save_stats(self):
        """
        Save current statistics to file.
        
        Returns:
            bool: True if stats were saved successfully, False otherwise
        """
        try:         
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, indent=2, cls=SetEncoder)
            
            logging.getLogger('manager').info(f"Saved global stats to {self.stats_file}")
            return True
        except Exception as e:
            logging.getLogger('manager').error(f"Error saving global stats: {str(e)}")
            return False
    
    def generate_final_summary(self):
        """
        Generate and save final summary report.
        
        Returns:
            dict or None: Final summary dictionary if successful, None otherwise
        """
        # Calculate derived metrics
        match_percentage = 0
        if self.stats['total_products'] > 0:
            match_percentage = (self.stats['matched_products'] / self.stats['total_products']) * 100
        
        final_summary = {
            'total_pages': self.stats['total_pages'],
            'total_products': self.stats['total_products'],
            'matched_products': self.stats['matched_products'],
            'match_percentage': match_percentage,
            'total_batches': self.stats['total_batches'],
            'completion_date': datetime.now().isoformat(),
            'batch_summaries': self.stats['batch_summaries']
        }
        
        # Save the final summary
        final_summary_file = os.path.join(self.output_dir, 'final_summary.json')
        try:
            with open(final_summary_file, 'w', encoding='utf-8') as f:
                json.dump(final_summary, f, indent=2)
            
            logging.getLogger('manager').info(f"Generated final summary at {final_summary_file}")
            logging.getLogger('manager').info(
                f"Final stats: {self.stats['total_pages']} pages, "
                f"{self.stats['total_products']} products, "
                f"{self.stats['matched_products']} matched ({match_percentage:.2f}%)"
            )
            return final_summary
        except Exception as e:
            logging.getLogger('manager').error(f"Error saving final summary: {str(e)}")
            return None 