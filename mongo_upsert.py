#!/usr/bin/env python3
"""
A reusable module to provide easy read/write access to a MongoDB collection.
"""
import os
import sys
import argparse
import time
import logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIBS_DIR = os.path.join(BASE_DIR, "libs")
sys.path.insert(0, LIBS_DIR)

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoUpsert:
    """A class to handle upsert operations to a MongoDB collection."""

    def __init__(self, connection_string, db_name, collection_name, connect_timeout=3600, logger="database"):
        """
        Initializes the MongoUpsert object and connects to the database.

        :param connection_string: The MongoDB connection string.
        :param db_name: The name of the database.
        :param collection_name: The name of the collection.
        :param connect_timeout: Timeout in seconds for connection retries.
        :param logger: An optional logger instance.
        """
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        self.logger = logger or logging.getLogger(__name__)
        self.connect(timeout=connect_timeout)

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context and close the connection."""
        self.close()

    def connect(self, timeout=3600):
        """
        Connects to the MongoDB database, retrying until the timeout is reached.
        Raises ConnectionFailure if the connection fails after the timeout.
        """
        start_time = time.time()
        while True:
            try:
                self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000) # 5 second timeout for server selection
                # The ismaster command is cheap and does not require auth.
                self.client.admin.command('ismaster')
                self.db = self.client[self.db_name]
                self.collection = self.db[self.collection_name]
                self.logger.info("Successfully connected to MongoDB.")
                return  # Success
            except ConnectionFailure as e:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    self.logger.error(f"Could not connect to MongoDB after {timeout} seconds.")
                    raise ConnectionFailure(f"Failed to connect to MongoDB after {timeout}s") from e
                
                self.logger.warning(f"Connection to MongoDB failed, retrying in 5 seconds... ({e})")
                time.sleep(5)

    def close(self):
        """Closes the connection to the MongoDB database."""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed.")

    def upsert(self, filter_query, data_to_upsert):
        """
        Performs an upsert operation (update or insert).

        :param filter_query: The filter to find the document to update.
        :param data_to_upsert: The data to insert or update with.
        :return: The result of the update operation.
        """
        if self.collection is None:
            raise Exception("Not connected to MongoDB. Call connect() first.")

        update = {"$set": data_to_upsert}
        result = self.collection.update_one(filter_query, update, upsert=True)
        return result

def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(description="Upsert data into a MongoDB collection.")
    parser.add_argument("--connection_string", required=True, help="MongoDB connection string.")
    parser.add_argument("--db_name", required=True, help="Name of the database.")
    parser.add_argument("--collection_name", required=True, help="Name of the collection.")
    parser.add_argument("--filter_query", required=True, help="JSON string for the filter query.")
    parser.add_argument("--data", required=True, help="JSON string of the data to upsert.")
    parser.add_argument("--connect_timeout", type=int, default=3600, help="Timeout in seconds for connection retries.")
    parser.add_argument("--log_level", default="DEBUG", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=args.log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Example of how to use it:
    # python mongo_upsert.py --connection_string "mongodb://localhost:27017/" --db_name "my_db" --collection_name "my_collection" --filter_query '{"id": "123"}' --data '{"name": "test", "value": "456"}'

    import json
    filter_q = json.loads(args.filter_query)
    data_to_upsert = json.loads(args.data)

    try:
        with MongoUpsert(args.connection_string, args.db_name, args.collection_name, connect_timeout=args.connect_timeout, logger=logger) as mongo_handler:
            upsert_result = mongo_handler.upsert(filter_q, data_to_upsert)
            logger.info(f"Upsert successful. Matched: {upsert_result.matched_count}, Modified: {upsert_result.modified_count}, Upserted ID: {upsert_result.upserted_id}")
    except Exception as e:
        logger.critical(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
