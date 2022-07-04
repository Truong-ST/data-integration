import json
import logging
import os
from tqdm import tqdm
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from collections import OrderedDict
from typing import List, Text, Dict, Union
from vimap.schema.bus_stop import BusStop

from vimap.db_api.constrains import *
from vimap.db_api.connect_db import MongoConnector
from vimap.db_api.db_place_api import PlaceCollector
db_connector = MongoConnector()

db = db_connector.connect_to_mongo_db()
bus_stop_coll = db[BUS_STOP_COLLECTION]


class BusStopCollector(PlaceCollector):
    name = BUS_STOP_COLLECTION

    def __init__(
            self,
            bus_stop_coll=bus_stop_coll,
            **kwargs
    ):
        super(BusStopCollector, self).__init__(**kwargs)
        self.bus_stop_coll = bus_stop_coll

    def init_db_bus_stop_from_csv(self, file_paths: Union[Text, List]):
        if isinstance(file_paths, Text):
            file_paths = [file_paths]

        for file in file_paths:
            df = pd.read_csv(file)
            for _, row in tqdm(df.iterrows(), total=len(df)):
                record = row.to_dict()
                self.bus_stop_coll.insert_one(record)

    def add_new_bus_stop(self, bus_stop: BusStop):
        self.add_new_place(bus_stop)
        self.bus_stop_coll.insert_one(bus_stop.place_info())

    def get_all_bus_stop(self, limit: int = None):
        if limit:
            data = self.bus_stop_coll.find().limit(limit=limit)
        else:
            data = self.bus_stop_coll.find()
        data = list(data)
        return data

    def get(self, limit: int = None):
        data = self.get_all_bus_stop(limit)
        for sample in data:
            info = self.find_by_latlon(sample["longitude"], sample["latitude"])
            sample.update(info)
        return data

    def find_bus_stop_info(self, place_id):
        return self.bus_stop_coll.find_one({"id": place_id})



