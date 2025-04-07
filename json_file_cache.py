class JsonFileCache:
    """
    A persistent cache that stores products in a JSON file with enhanced metrics.
    Loads all data into memory at startup and periodically saves to disk.
    Tracks different types of cache hits (matched vs. unmatched products).
    """
    
    def __init__(self, cache_file="product_cache.json", save_frequency=500):
        """
        Initialize the file-based cache.
        
        Args:
            cache_file (str): Path to the JSON file for storing cache
            save_frequency (int): How often to save to disk (after every N additions)
        """
        self.cache_file = cache_file
        self.save_frequency = save_frequency
        self.products = {}  # URL -> product dictionary
        self.hits = 0
        self.matched_hits = 0
        self.unmatched_hits = 0
        self.misses = 0
        self.additions_since_save = 0
        
        # Load existing cache if available
        self._load_cache()
    
    def _load_cache(self):
        """Load the cache from disk with enhanced statistics"""
        import os
        import json
        
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8', buffering=16777216) as f:
                    cache_data = json.load(f)
                    
                # Extract products and stats
                self.products = cache_data.get('products', {})
                self.hits = cache_data.get('hits', 0)
                self.matched_hits = cache_data.get('matched_hits', 0)
                self.unmatched_hits = cache_data.get('unmatched_hits', 0)
                self.misses = cache_data.get('misses', 0)
                
                # Count matched vs unmatched products in cache
                matched_count = sum(1 for p in self.products.values() if p.get('matched', False))
                
                print(f"Loaded {len(self.products)} products from cache file ({matched_count} matched)")
                print(f"Loaded cache stats: hits={self.hits}, matched_hits={self.matched_hits}, misses={self.misses}")
            except Exception as e:
                print(f"Error loading cache file: {str(e)}")
                # Start with an empty cache if there's an error
                self.products = {}
                self.hits = 0
                self.matched_hits = 0
                self.unmatched_hits = 0
                self.misses = 0
    
    def _save_cache(self, force=False):
        """
        Save the cache to disk with enhanced metrics.
        
        Args:
            force (bool): If True, save regardless of frequency counter
        """
        import json
        
        # Only save if forced or we've hit the frequency threshold
        if not force and self.additions_since_save < self.save_frequency:
            return
            
        try:
            # Count matched products for statistics
            matched_count = sum(1 for p in self.products.values() if p.get('matched', False))
            
            # Prepare complete cache data including enhanced stats
            cache_data = {
                'products': self.products,
                'hits': self.hits,
                'matched_hits': self.matched_hits,
                'unmatched_hits': self.unmatched_hits,
                'misses': self.misses,
                'cache_info': {
                    'size': len(self.products),
                    'matched_count': matched_count,
                    'last_saved': self._get_timestamp()
                }
            }
            
            # Write to a temporary file first
            temp_file = f"{self.cache_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f)
                
            # Then rename to the actual file (safer against corruption)
            import os
            if os.path.exists(self.cache_file):
                os.remove(self.cache_file)
            os.rename(temp_file, self.cache_file)
            
            # Reset the counter
            self.additions_since_save = 0
            
            print(f"Saved {len(self.products)} products to cache file ({matched_count} matched)")
            print(f"Saved cache stats: hits={self.hits}, matched_hits={self.matched_hits}, misses={self.misses}")
        except Exception as e:
            print(f"Error saving cache to file: {str(e)}")
    
    def _get_timestamp(self):
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def has_product(self, url):
        """
        Check if a product URL exists in the cache.
        
        Args:
            url (str): Product URL
            
        Returns:
            bool: True if the product is in the cache
        """
        return url in self.products
    
    def get_product(self, url):
        """
        Get a product from the cache with enhanced hit tracking.
        
        Args:
            url (str): Product URL
            
        Returns:
            dict or None: The product data if found, None otherwise
        """
        if url in self.products:
            self.hits += 1
            product = self.products[url]
            
            # Track matched vs unmatched hits
            if product.get('matched', False):
                self.matched_hits += 1
            else:
                self.unmatched_hits += 1
                
            # Save periodically based on hits to ensure stat persistence
            if self.hits % 50 == 0:
                self._save_cache(force=True)
                
            return product
        else:
            self.misses += 1
            return None
    
    def get_hit_ratio(self):
        """Calculate the hit ratio"""
        total_requests = self.hits + self.misses
        return self.hits / total_requests if total_requests > 0 else 0
    
    def get_effective_hit_ratio(self):
        """Calculate ratio of matched hits (most valuable for performance)"""
        total_requests = self.hits + self.misses
        return self.matched_hits / total_requests if total_requests > 0 else 0
    
    def add_product(self, product):
        """
        Add a product to the cache.
        
        Args:
            product (dict): Product data including 'url' key
            
        Returns:
            bool: True if the product was successfully added
        """
        if 'url' not in product:
            print("Product missing URL, cannot cache")
            return False
        
        self.products[product['url']] = product
        self.additions_since_save += 1
        
        # Periodically save to disk
        self._save_cache()
        
        return True
    
    def get_stats(self):
        """
        Get enhanced cache usage statistics.
        
        Returns:
            dict: Detailed statistics about cache usage
        """
        total_requests = self.hits + self.misses
        hit_ratio = self.get_hit_ratio()
        effective_hit_ratio = self.get_effective_hit_ratio()
        
        # Count matched products in cache
        matched_count = sum(1 for p in self.products.values() if p.get('matched', False))
        
        stats = {
            'size': len(self.products),
            'matched_count': matched_count,
            'unmatched_count': len(self.products) - matched_count,
            'hits': self.hits,
            'matched_hits': self.matched_hits,
            'unmatched_hits': self.unmatched_hits,
            'misses': self.misses,
            'hit_ratio': hit_ratio,
            'effective_hit_ratio': effective_hit_ratio
        }
        
        print(f"Cache stats: {stats['size']} items ({stats['matched_count']} matched), " 
              f"{stats['hits']} hits ({stats['matched_hits']} matched hits), "
              f"{hit_ratio:.2%} hit ratio, {effective_hit_ratio:.2%} effective hit ratio")
        
        return stats
    
    def clear(self):
        """Clear the cache"""
        self.products.clear()
        self._save_cache(force=True)
        
    def __len__(self):
        """Return the number of cached products"""
        return len(self.products)
        
    def __del__(self):
        """Save cache when the object is destroyed"""
        self._save_cache(force=True)