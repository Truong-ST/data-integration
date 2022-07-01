import json
import logging
import os
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from pymongo.errors import ServerSelectionTimeoutError
from collections import OrderedDict
from vimap.constrains import MONGO_URI, MONGO_DB


class MongoConnector:
    def __init__(self, db=None):
        self.db = db

    def connect_to_mongo_db(self):
        try:
            if self.db is None:
                client = MongoClient(MONGO_URI)
                self.db = client[MONGO_DB]
                return self.db
        except ServerSelectionTimeoutError as err:
            return err
