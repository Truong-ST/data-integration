import json
import logging
import os

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from collections import OrderedDict
from typing import List, Text, Dict, Union

from vimap.constrains import MONGO_URI, PLACE_COLLECTION
from vimap.db_api.connect_db import MongoConnector

db_connector = MongoConnector()

db = db_connector.connect_to_mongo_db()


def init_db_place_from_csv(file_paths: Union[Text, List]):
    place_connection = db[PLACE_COLLECTION]

    if isinstance(file_paths, Text):
        file_paths = [file_paths]

    for file in file_paths:
        df = pd.read_csv(file)
        for _, row in df.iterrows():
            record = row.to_dict()
            place_connection.insert_one(record)


def add_new_place():
    pass


def get_all_place():
    pass


init_db_place_from_csv("data/Hotels_Downtown.csv")
