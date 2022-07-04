import json
import logging
import os
from tqdm import tqdm
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from collections import OrderedDict
from typing import List, Text, Dict, Union
from vimap.schema.station import Station

from vimap.db_api.constrains import *
from vimap.db_api.connect_db import MongoConnector
from vimap.db_api.db_place_api import PlaceCollector
db_connector = MongoConnector()

db = db_connector.connect_to_mongo_db()
station_coll = db[STATION_COLLECTION]


class StationCollector(PlaceCollector):
    name = STATION_COLLECTION

    def __init__(
            self,
            station_coll=station_coll,
            **kwargs
    ):
        super(StationCollector, self).__init__(**kwargs)
        self.station_coll = station_coll

    def init_db_station_from_csv(self, file_paths: Union[Text, List]):
        if isinstance(file_paths, Text):
            file_paths = [file_paths]

        for file in file_paths:
            df = pd.read_csv(file)
            for _, row in tqdm(df.iterrows(), total=len(df)):
                record = row.to_dict()
                self.station_coll.insert_one(record)

    def add_new_station(self, station: Station):
        self.add_new_place(station)
        self.station_coll.insert_one(station.place_info())

    def get_all_station(self, limit: int = None):
        if limit:
            data = self.station_coll.find().limit(limit=limit)
        else:
            data = self.station_coll.find()
        data = list(data)
        return data

    def get(self, limit: int = None):
        data = self.get_all_station(limit)
        for sample in data:
            info = self.find_by_latlon(sample["longitude"], sample["latitude"])
            sample.update(info)
        return data

    def find_station_info(self, place_id):
        return self.station_coll.find_one({"id": place_id})



