"""
Logging Setup Module for Khair Scraper

This module provides a clean logging system with both console and file output.
All components can use this central logging configuration.
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logging(output_dir=None):
    """
    Set up a clean logging system with both console and file output.
    
    Args:
        output_dir (str, optional): Directory to store log files. If provided, creates a run-specific log file.
    
    Returns:
        dict: Dictionary containing logger objects and configuration information.
    """
    
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
    timestamp_str = None 
    # If an output directory is provided, create a run-specific log file too
    if output_dir:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_log_file = os.path.join(output_dir, f"OptimizedProductSpider.log")
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
        "timestamp_str": timestamp_str
    }

def get_logger(name):
    """
    Get a logger with the specified name.
    
    Args:
        name (str): Name of the logger to retrieve
        
    Returns:
        logging.Logger: Logger object with the specified name
    """
    return logging.getLogger(name) 