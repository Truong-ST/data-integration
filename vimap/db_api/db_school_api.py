import json
import logging
import os
from tqdm import tqdm
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from collections import OrderedDict
from typing import List, Text, Dict, Union
from vimap.schema.school import School

from vimap.db_api.constrains import *
from vimap.db_api.connect_db import MongoConnector
from vimap.db_api.db_place_api import PlaceCollector
db_connector = MongoConnector()

db = db_connector.connect_to_mongo_db()
school_coll = db[SCHOOL_COLLECTION]


class SchoolCollector(PlaceCollector):
    name = SCHOOL_COLLECTION
    def __init__(
            self,
            school_coll=school_coll,
            **kwargs
    ):
        super(SchoolCollector, self).__init__(**kwargs)
        self.school_coll = school_coll

    def init_db_school_from_csv(self, file_paths: Union[Text, List]):
        if isinstance(file_paths, Text):
            file_paths = [file_paths]

        for file in file_paths:
            df = pd.read_csv(file)
            for _, row in tqdm(df.iterrows(), total=len(df)):
                record = row.to_dict()
                self.school_coll.insert_one(record)

    def add_new_school(self, school: School):
        self.add_new_place(school)
        self.school_coll.insert_one(school.place_info())

    def get_all_school(self, limit: int = None):
        if limit:
            data = self.school_coll.find().limit(limit=limit)
        else:
            data = self.school_coll.find()
        data = list(data)
        return data

    def get(self, limit: int = None):
        data = self.get_all_school(limit)
        for sample in data:
            info = self.find_by_latlon(sample["longitude"], sample["latitude"])
            sample.update(info)
        return data

    def find_school_info(self, place_id):
        return self.school_coll.find_one({"id": place_id})



