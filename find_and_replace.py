#!/usr/bin/env python3
"""
Find and replace strings in a JSON report file containing an array of objects.

This script reads a JSON file with an array of objects, performs find and replace
operations on a specified key in each object, and overwrites the file with the modified content.
"""

import json
import argparse
import sys
import os
import logging


def find_and_replace(report_path, find_string, replace_string, key='original_file'):
    """
    Find and replace strings in a JSON report file containing an array of objects.
    
    Args:
        report_path: Path to JSON file containing array of objects
        find_string: String to find
        replace_string: String to replace with
        key: Key in each object to perform find/replace on (default: 'original_file')
        
    Returns:
        Tuple of (return_code, message)
        0: Success
        1: File read error
        2: Invalid JSON or not array of objects
        3: Unexpected error
    """
    logger = logging.getLogger(__name__)
    try:
        # Check if file exists
        if not os.path.exists(report_path):
            logger.error(f"Report file not found: {report_path}")
            return 1, f"Report file not found: {report_path}"
        
        # Read JSON file
        logger.info(f"Reading JSON file: {report_path}")
        with open(report_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.debug(f"Loaded {len(data)} objects from JSON file")
        
        # Validate it's an array
        if not isinstance(data, list):
            logger.error("JSON file must contain an array")
            return 2, "Report file must contain a JSON array"
        
        # Validate all elements are objects
        if not all(isinstance(item, dict) for item in data):
            logger.error("Not all elements in the array are objects")
            return 2, "All elements in the array must be objects"
        
        # Perform find and replace on specified key
        logger.info(f"Starting find and replace: '{find_string}' -> '{replace_string}' in key '{key}'")
        modified_count = 0
        for item in data:
            if key in item and isinstance(item[key], str):
                original_value = item[key]
                item[key] = item[key].replace(find_string, replace_string)
                if original_value != item[key]:
                    modified_count += 1
                    logger.debug(f"Modified: {original_value} -> {item[key]}")
        
        # Overwrite the file
        logger.info(f"Overwriting file: {report_path}")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        success_msg = f"Successfully replaced '{find_string}' with '{replace_string}' in {modified_count} objects (key: '{key}') in {report_path}"
        logger.info(success_msg)
        return 0, success_msg
    
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON format: {str(e)}"
        logger.error(error_msg)
        return 2, error_msg
    except IOError as e:
        error_msg = f"File read/write error: {str(e)}"
        logger.error(error_msg)
        return 1, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return 3, error_msg


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.debug("Starting find_and_replace script")
    
    parser = argparse.ArgumentParser(
        description='Find and replace strings in a JSON report file containing an array of objects'
    )
    parser.add_argument('report_path', help='Path to JSON report file')
    parser.add_argument('find_string', help='String to find')
    parser.add_argument('replace_string', help='String to replace with')
    parser.add_argument('--key', default='original_file', help='Key in each object to perform replacement (default: original_file)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Update logging level if debug flag is set
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    logger.info(f"Arguments: report_path={args.report_path}, find_string={args.find_string}, replace_string={args.replace_string}, key={args.key}")
    
    code, message = find_and_replace(
        args.report_path,
        args.find_string,
        args.replace_string,
        args.key
    )
    
    if code == 0:
        print(message, file=sys.stdout)
    else:
        print(message, file=sys.stderr)
    
    logger.debug(f"Exiting with code: {code}")
    sys.exit(code)


if __name__ == '__main__':
    main()
