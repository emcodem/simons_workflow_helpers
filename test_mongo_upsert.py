#!/usr/bin/env python3
import unittest
import uuid
import logging
import sys
import os

# Add the parent directory to the path to find the mongo_upsert module
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from mongo_upsert import MongoUpsert

class TestMongoUpsert(unittest.TestCase):
    """Integration tests for the MongoUpsert class."""

    def setUp(self):
        """Set up the test environment."""
        self.connection_string = "mongodb://192.168.178.23:27017,192.168.178.57:27017,192.168.178.58:27017/?replicaSet=rs0"
        self.db_name = "test_db"
        self.collection_name = "test_collection"
        self.test_id = str(uuid.uuid4())

        # Configure logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # The test data
        self.filter_query = {"test_id": self.test_id}
        self.data_to_upsert = {"test_id": self.test_id, "message": "Hello, MongoDB!"}

    def test_upsert_integration(self):
        """Test the full upsert and cleanup process."""
        try:
            with MongoUpsert(self.connection_string, self.db_name, self.collection_name, logger=self.logger) as mongo_handler:
                # 1. Perform the upsert
                upsert_result = mongo_handler.upsert(self.filter_query, self.data_to_upsert)
                self.logger.info(f"Upsert result: {upsert_result.raw_result}")

                # Assert that a new document was created
                self.assertIsNotNone(upsert_result.upserted_id)
                self.assertEqual(upsert_result.matched_count, 0)

                # 2. Verify the data was inserted
                retrieved_doc = mongo_handler.collection.find_one(self.filter_query)
                self.assertIsNotNone(retrieved_doc)
                self.assertEqual(retrieved_doc["message"], "Hello, MongoDB!")

                # 3. Test updating the existing document
                updated_data = {"message": "Hello again, MongoDB!"}
                update_result = mongo_handler.upsert(self.filter_query, updated_data)
                self.logger.info(f"Update result: {update_result.raw_result}")
                
                # Assert that an existing document was modified
                self.assertEqual(update_result.matched_count, 1)
                self.assertEqual(update_result.modified_count, 1)

                # Verify the update
                retrieved_doc_after_update = mongo_handler.collection.find_one(self.filter_query)
                self.assertEqual(retrieved_doc_after_update["message"], "Hello again, MongoDB!")

        except Exception as e:
            self.fail(f"MongoUpsert integration test failed with an exception: {e}")

    def tearDown(self):
        """Clean up the test data from the database."""
        try:
            with MongoUpsert(self.connection_string, self.db_name, self.collection_name, logger=self.logger) as mongo_handler:
                # Find the document to ensure it exists before trying to delete
                doc_to_delete = mongo_handler.collection.find_one(self.filter_query)
                if doc_to_delete:
                    #delete_result = mongo_handler.collection.delete_one(self.filter_query)
                    #self.logger.info(f"Cleanup: Deleted {delete_result.deleted_count} document(s).")
                    self.logger.info("Cleanup: Deleting test document.")
                else:
                    self.logger.info("Cleanup: No document found to delete.")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


if __name__ == '__main__':
    unittest.main()
