"""
Custom Retry Middleware Module for Khair Scraper

This module provides enhanced retry functionality for Scrapy requests,
implementing exponential backoff and jitter for more effective retries.
"""

import time
import random
import logging
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

class CustomRetryMiddleware(RetryMiddleware):
    """
    Custom middleware that implements exponential backoff for retries.
    
    This middleware extends Scrapy's RetryMiddleware to add exponential backoff
    with jitter, making retries more effective in case of server throttling.
    """
    
    def __init__(self, settings):
        """
        Initialize the middleware with settings.
        
        Args:
            settings: Scrapy settings object
        """
        super(CustomRetryMiddleware, self).__init__(settings)
        self.max_retry_times = settings.getint('RETRY_TIMES', 3)
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES', [500, 502, 503, 504, 429]))
        self.priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST', -1)
        # Base delay in seconds
        self.base_delay = settings.getfloat('RETRY_BASE_DELAY', 1)
        # Maximum delay in seconds
        self.max_delay = settings.getfloat('RETRY_MAX_DELAY', 60)

    def get_retry_request(self, request, reason, spider):
        """
        Get a request with applied backoff for retry.
        
        Args:
            request: The failed request
            reason: Reason for the retry
            spider: The spider making the request
            
        Returns:
            Request: A new request for retry, or None if max retries reached
        """
        retry_count = request.meta.get('retry_times', 0) + 1
        
        # Calculate exponential backoff delay
        delay = min(self.max_delay, self.base_delay * (2 ** retry_count))
        # Add jitter to avoid synchronized requests
        delay = delay * (0.5 + random.random())
        
        spider.logger.info(f"Retry {retry_count} for {request.url}, backing off for {delay:.2f} seconds")
        time.sleep(delay)
        
        return super(CustomRetryMiddleware, self).get_retry_request(request, reason, spider) 