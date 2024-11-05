from pymongo import MongoClient

from utils.config import ConfigReader
from utils.logger import log_error


class MongoDB:
    _client = None
    _db_name = None
    _instance = None  # Singleton instance

    def __new__(cls):
        # Singleton pattern to ensure only one instance is created
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        # Prevent reinitialization
        if not self._initialized:
            self._client = None
            host = ConfigReader().get_mongo_host()
            port = ConfigReader().get_mongo_port()
            self._client = MongoClient(f"mongodb://{host}:{port}/")
            self._db_name = ConfigReader().get_mongo_db_name()
            self._initialized = True

    def upsert(self, collection: str, id_object: any, object: any):
        db = self._client[self._db_name]
        collection = db[collection]

        upsert = {
            "$set": object
        }
        try:
            collection.update_one(id_object, upsert, upsert=True)
        except Exception as e:
            log_error(f"An error occurred while updating the document: {e}")
            return None

    def find_one(self, collection: str, id_object: any):
        db = self._client[self._db_name]
        collection = db[collection]
        try:
            document = collection.find_one(id_object)
            return document
        except Exception as e:
            log_error(f"An error occurred while retrieving the document: {e}")
            return None
