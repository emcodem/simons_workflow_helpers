#!/usr/bin/env python3
"""
Add a field to a report JSON entry safely using SoftFileLock,
with stale lock detection (force remove if older than 10 seconds).
"""

import sys
import json
import os
import argparse
import time
import logging

# Resolve custom libs folder if using --target
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIBS_DIR = os.path.join(BASE_DIR, "libs")
if LIBS_DIR not in sys.path:
    sys.path.insert(0, LIBS_DIR)

from mongo_upsert import MongoUpsert

def load_file_value(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    try:
        return json.loads(content)
    except Exception:
        return content

def main():
    parser = argparse.ArgumentParser(description="Add a field to a MongoDB document safely.")
    parser.add_argument("--connection_string", required=True, help="MongoDB connection string.")
    parser.add_argument("--db_name", required=True, help="MongoDB database name.")
    parser.add_argument("--collection_name", required=True, help="MongoDB collection name.")
    parser.add_argument("--match_field", required=True, help="Field name to match the document for updating.")
    parser.add_argument("--match_value", required=True, help="Value to identify the target entry in the match_field.")
    parser.add_argument("--value_to_add", required=True, help="String or path to a file (file contents added; JSON parsed if possible).")
    parser.add_argument("--value_from_file", required=False, action='store_true', help="Treat --value_to_add as a file path and load its contents.")
    parser.add_argument("--new_field_name", required=True, help="Name of the field to add to the matched document.")
    args = parser.parse_args()

    # Set up basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    added_value = args.value_to_add
    if args.value_from_file:
        try:
            added_value = load_file_value(args.value_to_add)
        except Exception as e:
            logger.error(f"Failed to load value from file '{args.value_to_add}': {e}")
            sys.exit(1)

    try:
        with MongoUpsert(
            connection_string=args.connection_string,
            db_name=args.db_name,
            collection_name=args.collection_name,
            logger=logger
        ) as mongo:
            # The query to find the document to update
            filter_query = {args.match_field: args.match_value}
            
            # The data to add/update in the document
            update_data = {args.new_field_name: added_value}

            # Perform the upsert operation
            result = mongo.upsert(filter_query, update_data)

            if result.matched_count > 0 or result.upserted_id is not None:
                logger.info(f"Successfully updated or inserted document in '{args.db_name}.{args.collection_name}'.")
                if result.upserted_id:
                    logger.info(f"New document inserted with ID: {result.upserted_id}")
                else:
                    logger.info(f"Matched {result.matched_count} document(s) and modified {result.modified_count}.")
            else:
                logger.warning(f"No document found matching query: {filter_query}")
                sys.exit(2) # Exit with code 2 if no document was found/updated

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
