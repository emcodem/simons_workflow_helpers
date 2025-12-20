"""
Lightweight shim of MongoUpsert for local testing.

This shim provides a minimal in-memory implementation used only for
testing `add_field_to_report.py` when a real MongoDB-backed
`mongo_upsert.py` isn't present.

It implements a context manager `MongoUpsert` with an `upsert`
method and a `collection.drop()` convenience method.
"""
from types import SimpleNamespace
import threading
import time

# Global in-memory stores keyed by (db_name, collection_name)
_STORES = {}
_LOCK = threading.Lock()
_ID_COUNTER = 1

class _CollectionShim:
    def __init__(self, store_key):
        self._store_key = store_key

    def drop(self):
        with _LOCK:
            _STORES[self._store_key] = []

class MongoUpsert:
    def __init__(self, connection_string, db_name, collection_name, logger=None, delete_after=None):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        self.logger = logger
        self.delete_after = delete_after
        self._key = (db_name, collection_name)
        with _LOCK:
            if self._key not in _STORES:
                _STORES[self._key] = []
        self.collection = _CollectionShim(self._key)

    def __enter__(self):
        if self.logger:
            self.logger.info("(shim) Connected to in-memory Mongo store")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.logger:
            self.logger.info("(shim) Closing in-memory Mongo store connection")
        return False

    def upsert(self, filter_query, update_data):
        """Perform an in-memory upsert. Returns a SimpleNamespace resembling pymongo result."""
        global _ID_COUNTER
        matched_count = 0
        modified_count = 0
        upserted_id = None

        with _LOCK:
            store = _STORES[self._key]
            # naive match: check if all key/value pairs exist in doc
            found = None
            for doc in store:
                ok = True
                for k, v in filter_query.items():
                    if doc.get(k) != v:
                        ok = False
                        break
                if ok:
                    found = doc
                    break

            now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            if found is None:
                # insert
                doc = {**filter_query}
                # apply update_data as direct fields (behaviour chosen for testing)
                doc.update(update_data)
                doc['date_created'] = now
                doc['date_modified'] = now
                doc['_id'] = f"shim-{_ID_COUNTER}"
                _ID_COUNTER += 1
                store.append(doc)
                upserted_id = doc['_id']
                matched_count = 0
                modified_count = 0
            else:
                # update
                for k, v in update_data.items():
                    found[k] = v
                found['date_modified'] = now
                matched_count = 1
                modified_count = 1

        # Return simple object with attributes expected by the caller
        return SimpleNamespace(
            matched_count=matched_count,
            modified_count=modified_count,
            upserted_id=upserted_id,
        )
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

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure


from datetime import datetime

class MongoUpsert:
    """A class to handle upsert operations to a MongoDB collection."""

    def __init__(self, connection_string, db_name, collection_name, connect_timeout=3600, logger=None, delete_after=259200):
        """
        Initializes the MongoUpsert object and connects to the database.

        :param connection_string: The MongoDB connection string.
        :param db_name: The name of the database.
        :param collection_name: The name of the collection.
        :param connect_timeout: Timeout in seconds for connection retries.
        :param logger: An optional logger instance.
        :param delete_after: Seconds after which documents are deleted. Defaults to 3 days. Set to 0 to disable.
        """
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        self.delete_after = delete_after
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

                # Ensure TTL index exists if delete_after is specified
                if self.delete_after and self.delete_after > 0:
                    self.collection.create_index(
                        [("date_created", ASCENDING)],
                        expireAfterSeconds=self.delete_after
                    )
                    self.logger.info(f"Ensured TTL index on 'date_created' with expiry of {self.delete_after} seconds.")

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
        - On insert, 'date_created' and 'date_modified' are added.
        - On update, 'date_modified' is updated.

        :param filter_query: The filter to find the document to update.
        :param data_to_upsert: The data to insert or update with.
        :return: The result of the update operation.
        """
        if self.collection is None:
            raise Exception("Not connected to MongoDB. Call connect() first.")

        now = datetime.utcnow()
        
        # Data to set on both insert and update
        update_set = {
            **data_to_upsert,
            'date_modified': now
        }

        # Data to set only on insert
        update_set_on_insert = {
            'date_created': now
        }

        update = {
            "$set": update_set,
            "$setOnInsert": update_set_on_insert
        }
        
        result = self.collection.update_one(filter_query, update, upsert=True)
        return result

def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(description="Upsert data into a MongoDB collection.")
    parser.add_argument("--connection_string", required=True, help="MongoDB connection string.")
    parser.add_argument("--db_name", required=True, help="Name of the database.")
    parser.add_argument("--collection_name", required=True, help="Name of the collection.")
    parser.add_argument("--filter_query", required=True, help="JSON string for the filter query.")
    parser.add_argument("--data_to_upsert", required=True, help="JSON string for the data to upsert.")
    parser.add_argument("--delete_after", type=int, default=259200, help="Seconds to keep documents. Default is 3 days. 0 to disable.")
    args = parser.parse_args()

    # Set up basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    try:
        filter_q = json.loads(args.filter_query)
        data = json.loads(args.data_to_upsert)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from arguments: {e}")
        sys.exit(1)

    try:
        with MongoUpsert(
            connection_string=args.connection_string,
            db_name=args.db_name,
            collection_name=args.collection_name,
            logger=logger,
            delete_after=args.delete_after
        ) as mongo:
            mongo.upsert(filter_q, data)
            logger.info("Upsert operation completed successfully.")
    except (ConnectionFailure, Exception) as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
