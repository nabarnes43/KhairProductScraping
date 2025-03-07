"""
Custom JSON Encoder Module for Khair Scraper

This module provides custom JSON encoders for serializing special data types.
"""

import json

class SetEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that can handle sets by converting them to lists.
    
    This encoder extends the default JSONEncoder to handle Python sets,
    which are not natively serializable to JSON.
    """
    def default(self, obj):
        """
        Convert custom objects to JSON serializable objects.
        
        Args:
            obj: The object to encode
            
        Returns:
            A JSON serializable version of the object
        """
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj) 