from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from utils.logger import Logger


class MongoDB:
    _client = None
    _db_name = None
    _instance = None  # Singleton instance

    def __new__(cls, bot_name: str, host=None, port=None, db_name=None):
        # Singleton pattern to ensure only one instance is created
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, bot_name: str, host=None, port=None, db_name=None):
        # Avoid reinitialization
        if not self._initialized:
            # If host and port are not provided, use those from ConfigReader
            self.logger = Logger.get_logger(bot_name)
            try:
                self._client = MongoClient(f"mongodb://{host}:{port}/")
                self._db_name = db_name
                self._initialized = True
            except Exception as e:
                self.logger.error(f"Error connecting to MongoDB: {e}")
                raise

    def upsert(self, collection: str, id_object: any, payload: any):
        db = self._client[self._db_name]
        collection = db[collection]

        upsert_operation = {
            "$set": payload
        }
        try:
            result = collection.update_one(id_object, upsert_operation, upsert=True)
            return result.upserted_id if result.upserted_id else result.modified_count
        except Exception as e:
            self.logger.error(f"An error occurred while updating the document: {e}")
            return None

    def find_one(self, collection: str, id_object: any):
        db = self._client[self._db_name]
        collection = db[collection]
        try:
            document = collection.find_one(id_object)
            return document
        except Exception as e:
            self.logger.error(f"An error occurred while retrieving the document: {e}")
            return None

    def test_connection(self):
        """
        Tests the connection to MongoDB by executing a ping command.
        Returns True if the connection is successful, otherwise False.
        """
        try:
            # The admin database is always present
            self._client.admin.command('ping')
            print("Successfully connected to MongoDB.")
            return True
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error during MongoDB connection test: {e}")
            return False
