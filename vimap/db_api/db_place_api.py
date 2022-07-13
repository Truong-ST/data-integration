import json
import logging
import os
from tqdm import tqdm
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from collections import OrderedDict
from typing import List, Text, Dict, Union

from vimap.db_api.constrains import MONGO_URI
from vimap.db_api.constrains import *
from vimap.db_api.connect_db import MongoConnector
from vimap.schema.place import Place
db_connector = MongoConnector()

db = db_connector.connect_to_mongo_db()
place_coll = db[PLACE_COLLECTION]


class PlaceCollector:
    name: PLACE_COLLECTION

    def __init__(
            self,
            place_coll=place_coll,
            **kwargs
    ):
        self.place_coll = place_coll

    def find_by_latlon(
            self,
            longitude,
            latitude
    ):
        _filter = {
            "longitude": longitude,
            "latitude": latitude
        }
        sample = self.place_coll.find_one(_filter)
        return sample

    def check_place_is_exist(self, place: Place):
        sample = self.find_by_latlon(place.longitude, place.latitude)
        if sample:
            return True

        return False

    def init_db_place_from_csv(self, file_paths: Union[Text, List]):
        if isinstance(file_paths, Text):
            file_paths = [file_paths]

        for file in file_paths:
            df = pd.read_csv(file)
            for _, row in tqdm(df.iterrows(), total=len(df)):
                record = row.to_dict()
                self.place_coll.insert_one(record)

    def add_new_place(self, place: Place):
        if not self.check_place_is_exist(place):
            print(f"DB: >> Insert {place.place_info()}")
            self.place_coll.insert_one(place.place_info())

    def get_all_place(self, limit: int = None):
        if limit:
            samples = self.place_coll.find().limit(limit=limit)
        else:
            samples = self.place_coll.find()
        samples = list(samples)
        return samples

    def get(self, limit: int = None, _filter: Dict = None):
        if _filter is None:
            _filter = {}
        if limit is not None:
            samples = self.place_coll.find(_filter).limit(limit=limit)
        else:
            samples = self.place_coll.find(_filter)
        samples = list(samples)
        return samples
