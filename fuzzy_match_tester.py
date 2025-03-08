#!/usr/bin/env python3
"""
Fuzzy Match Tester - A utility to test fuzzy matching thresholds on product names

This script allows you to compare input text against entries in a reference dataset (skinsort_shampoos.json)
using fuzzy string matching. It shows similarity scores across different thresholds to help determine 
the optimal threshold for your scraper without running a full scrape.
"""

import json
import argparse
import sys
import re
from rapidfuzz import fuzz, process
from typing import List, Dict, Any, Tuple, Optional


class FuzzyMatchTester:
    """
    A utility class to test fuzzy matching with different thresholds against a reference dataset.
    
    This class helps determine the optimal similarity threshold for fuzzy matching by allowing
    users to test individual strings against the reference dataset and see results at various
    threshold levels.
    """
    
    def __init__(self, reference_file: str):
        """
        Initialize the FuzzyMatchTester with a reference JSON file.
        
        Args:
            reference_file (str): Path to the JSON file containing reference products
        """
        self.reference_file = reference_file
        self.reference_products = []
        self.normalized_products = []
        self.product_data = []
        self.load_reference_data()
        
    def normalize_text(self, text: str) -> str:
        """
        Normalize text for better matching by handling special characters and common variations.
        
        Args:
            text (str): Text to normalize
            
        Returns:
            str: Normalized text
        """
        # Convert to lowercase
        text = text.lower()
        
        # Replace '&' with 'and'
        text = text.replace('&', ' and ')
        
        # Handle other special characters and common variations
        text = text.replace('-', ' ')
        text = text.replace('/', ' ')
        
        # Remove registered/trademark symbols
        text = text.replace('®', '')
        text = text.replace('™', '')
        
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
        
    def load_reference_data(self) -> None:
        """
        Load product data from the reference JSON file.
        
        The JSON file should contain a list of product objects, each with 'brand' and 'name' fields.
        Removes duplicate entries to ensure unique product names.
        """
        try:
            with open(self.reference_file, 'r', encoding='utf-8') as f:
                self.product_data = json.load(f)
                
                # Validate the data structure
                if not isinstance(self.product_data, list) or not all(isinstance(item, dict) for item in self.product_data):
                    raise ValueError(f"Expected a list of product objects, got: {type(self.product_data)}")
                    
                if not all('brand' in item and 'name' in item for item in self.product_data):
                    raise ValueError("JSON list items must contain 'brand' and 'name' fields")
                
                # Create a list of full product names for matching, removing duplicates
                full_products = [f"{p['brand']} {p['name']}" for p in self.product_data]
                unique_products = list(dict.fromkeys(full_products))  # Remove duplicates while preserving order
                
                self.reference_products = unique_products
                
                # Create normalized versions for better matching
                self.normalized_products = [self.normalize_text(p) for p in unique_products]
                
                original_count = len(full_products)
                unique_count = len(unique_products)
                
                print(f"Loaded {unique_count} unique products from {self.reference_file}")
                if original_count > unique_count:
                    print(f"Note: Removed {original_count - unique_count} duplicate entries")
                
        except FileNotFoundError:
            print(f"Error: File not found: {self.reference_file}")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in file: {self.reference_file}")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading reference data: {str(e)}")
            sys.exit(1)
    
    def test_match(self, test_text: str, show_threshold: int = 70, top_matches: int = 5) -> None:
        """
        Test a string against the reference dataset and display matching results.
        
        Args:
            test_text (str): The text to test for matching
            show_threshold (int): Show matches above this threshold (0-100)
            top_matches (int): Number of top matches to display
        """
        if not self.reference_products:
            print("No reference products loaded. Cannot perform matching.")
            return
        
        # Check for suspiciously short input that might indicate shell parsing errors
        suspicious_terms = ["head", "head shoulders", "h s", "dove", "loreal", "l'oreal"]
        if len(test_text.split()) <= 1 or test_text.lower() in suspicious_terms:
            print("\n⚠️  WARNING: Your input seems unusually short or incomplete.")
            print("   If you're trying to search for a product with special characters like '&',")
            print("   the shell may have misinterpreted your command.")
            print("\n   Try using quotes around your search text:")
            print(f'   ./fuzzy_match_tester.py "{test_text} & Shoulders"')
            print("\n   Or use interactive mode instead:")
            print("   ./fuzzy_match_tester.py")
            print("   Then type your search text when prompted.")
            print("\n   Continuing with current input anyway...\n")
            
        try:
            # Normalize the test text
            normalized_test_text = self.normalize_text(test_text)
            
            # Try multiple fuzzy matching algorithms to find the best match
            ratio_matches = process.extract(
                normalized_test_text, 
                self.normalized_products, 
                scorer=fuzz.ratio,  # Standard ratio
                limit=top_matches * 2  # Get more to combine results
            )
            
            token_set_matches = process.extract(
                normalized_test_text, 
                self.normalized_products, 
                scorer=fuzz.token_set_ratio,  # Better for word order differences
                limit=top_matches * 2  # Get more to combine results
            )
            
            # Partial ratio can be useful for products that contain the query as a substring
            partial_matches = process.extract(
                normalized_test_text, 
                self.normalized_products, 
                scorer=fuzz.partial_ratio,  # Good for substring matches
                limit=top_matches * 2  # Get more to combine results
            )
            
            # Combine results for better matching
            combined_matches = {}
            
            # Add all matches to the dictionary with their scores
            for match_list in [ratio_matches, token_set_matches, partial_matches]:
                for i, (norm_text, score, idx) in enumerate(match_list):
                    original_text = self.reference_products[idx]  # Get original non-normalized text
                    if original_text in combined_matches:
                        combined_matches[original_text] = max(combined_matches[original_text], score)
                    else:
                        combined_matches[original_text] = score
            
            # Sort by score
            matches = [(text, score, idx) for idx, (text, score) in 
                       enumerate(sorted(combined_matches.items(), key=lambda x: x[1], reverse=True)[:top_matches])]
            
            # Print test text
            print(f"\nTesting: '{test_text}'")
            print("-" * 80)
            
            # Calculate different threshold scenarios
            thresholds = [95, 90, 85, 80, 75, 70, 65, 60]
            
            # Print matches
            print(f"Top {min(len(matches), top_matches)} matches:")
            for i, (match_text, score, _) in enumerate(matches, 1):
                if score >= show_threshold:
                    is_match = "✓" if score >= 90 else " "
                    print(f"{i}. [{score:.2f}%] {is_match} {match_text}")
            
            print("\nThreshold Analysis:")
            print("-" * 80)
            
            for threshold in thresholds:
                # Count matches above threshold
                matching_products = sum(1 for _, score, _ in matches if score >= threshold)
                
                would_match = "MATCH" if matches and matches[0][1] >= threshold else "NO MATCH"
                
                print(f"Threshold {threshold}: {would_match} (Total {matching_products} products would match)")
                
            # Add recommendations based on the match quality
            print("\nRecommendations:")
            print("-" * 80)
            
            if not matches:
                print("No matches found at all. Check your query or data.")
            elif matches[0][1] >= 90:
                print(f"High-quality match found! Recommended threshold: 90")
            elif matches[0][1] >= 80:
                print(f"Good match found. Recommended threshold: 80")
            elif matches[0][1] >= 70:
                print(f"Fair match found. Recommended threshold: 70")
            else:
                print(f"Poor match quality. Consider using a lower threshold or improving your query.")
                
            # Special case for short names
            if len(test_text) < 15:
                print("\nNote: For short names (< 15 chars), a stricter threshold is recommended (90+)")
            
            print("-" * 80)
            
        except Exception as e:
            print(f"Error in fuzzy matching: {str(e)}")
    
    def interactive_mode(self) -> None:
        """Run an interactive session allowing the user to test multiple strings."""
        print(f"Interactive Fuzzy Match Tester")
        print(f"Reference data: {self.reference_file} ({len(self.reference_products)} unique products)")
        print(f"Type 'quit' or 'exit' to end the session\n")
        
        while True:
            try:
                test_input = input("Enter product name to test (or 'quit'): ").strip()
                
                if test_input.lower() in ('quit', 'exit', 'q'):
                    break
                    
                if not test_input:
                    continue
                
                # Parse additional options if provided with a colon
                if ':' in test_input:
                    parts = test_input.split(':', 1)
                    test_input = parts[0].strip()
                    
                    # Try to extract threshold and count
                    try:
                        options = parts[1].strip().split()
                        threshold = 70
                        count = 5
                        
                        for opt in options:
                            if opt.startswith('t='):
                                threshold = int(opt[2:])
                            elif opt.startswith('n='):
                                count = int(opt[2:])
                        
                        self.test_match(test_input, threshold, count)
                    except Exception as e:
                        print(f"Error parsing options: {str(e)}")
                        self.test_match(test_input)
                else:
                    self.test_match(test_input)
                    
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {str(e)}")


def main():
    """Main function to parse arguments and run the fuzzy match tester."""
    # Support both old-style arguments and new simplified style
    
    # First check for help flag
    if any(arg in ['-h', '--help'] for arg in sys.argv):
        show_help()
        return
        
    # Initialize defaults
    reference_file = 'skinsort_shampoos.json'
    test_text = None
    
    # Check for old-style arguments first (-f, -t)
    if '-f' in sys.argv:
        try:
            file_index = sys.argv.index('-f')
            if file_index + 1 < len(sys.argv):
                reference_file = sys.argv[file_index + 1]
        except (ValueError, IndexError):
            pass
            
    if '-t' in sys.argv:
        try:
            text_index = sys.argv.index('-t')
            if text_index + 1 < len(sys.argv):
                # Join all remaining arguments after -t
                test_text = ' '.join(sys.argv[text_index + 1:])
        except (ValueError, IndexError):
            pass
    
    # If no old-style arguments found, try new style
    if test_text is None and '-f' not in sys.argv and '-t' not in sys.argv and len(sys.argv) > 1:
        # If first argument is a JSON file, use it as the reference file
        if sys.argv[1].endswith('.json') and not sys.argv[1].startswith('-'):
            reference_file = sys.argv[1]
            # If more arguments exist, use them as the test text
            if len(sys.argv) > 2:
                test_text = ' '.join(sys.argv[2:])
        else:
            # Otherwise, use all arguments as the test text
            test_text = ' '.join(sys.argv[1:])
    
    # Create tester and run
    tester = FuzzyMatchTester(reference_file)
    
    if test_text:
        tester.test_match(test_text)
    else:
        tester.interactive_mode()

def show_help():
    """Display help information."""
    print("Fuzzy Match Tester - Test fuzzy matching thresholds on product names")
    print("\nUsage:")
    print("  1. Simple style (recommended):")
    print("     ./fuzzy_match_tester.py [filename.json] [text to match]")
    print("\n  2. Flag style (for backward compatibility):")
    print("     ./fuzzy_match_tester.py -f filename.json -t \"text to match\"")
    print("\nExamples:")
    print("  ./fuzzy_match_tester.py                             # Interactive mode with default file")
    print("  ./fuzzy_match_tester.py skinsort_shampoos.json      # Interactive mode with specified file")
    print("  ./fuzzy_match_tester.py Nizoral Anti Dandruff       # Match text using default file")
    print("  ./fuzzy_match_tester.py data.json Nizoral Shampoo   # Match text using specified file")
    print("  ./fuzzy_match_tester.py -f data.json -t \"Nizoral Shampoo\"   # Match with flags")
    print("  ./fuzzy_match_tester.py \"Head & Shoulders\"          # Use quotes for special characters")
    sys.exit(0)

if __name__ == '__main__':
    main() 