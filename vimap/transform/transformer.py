import os
from uuid import uuid4
import sys
import pandas as pd
from typing import Text, Dict, Union, List
from vimap.transform.data_mapper import *
from vimap.transform.data_matcher import *
from vimap.transform.schema_mapper import *
from vimap.transform.schema_matcher import *
from vimap.transform.schema_matcher.schema_matcher import SchemaMatcher
import numpy as np
from vimap.db_api import *
from vimap.schema import *

module = sys.modules[__name__]


class Transformer:
    def __init__(
            self,
            schema_matcher: SchemaMatcherBase = None,
            data_matcher: DataMatcherBase = None,
            data_mapper: DataMapperBase = None,
            **kwargs
    ):
        self.schema_matcher = schema_matcher or SchemaMatcher(use_flexmatcher=False)
        self.data_matcher = data_matcher or DataMatcher()
        self.data_mapper = data_mapper or DataMapper()
        self.schema_list = []

    def _check_keyword(self, text: Text, keywords: Union[Text, List]):
        if isinstance(keywords, Text):
            keywords = [keywords]

        for word in keywords:
            if word.lower() in text.lower():
                return True
        return False

    def _get_place_type_from_name(self, name):
        keywords = {
            "Station": ["rail", "station"],
            "School": ["School", "Academy"]
        }
        for place_type, words in keywords.items():
            if self._check_keyword(name, words):
                return place_type

        return None

    def process_samples(self, samples: Union[Place, List]):
        if isinstance(samples, Place):
            samples = [samples]

        for sample in samples:
            place_type = self._get_place_type_from_name(sample.name)
            sample.place_type = place_type
        return samples

    def get_sample_place_by_schema_matched(self, df, matched_schema):
        schema = "Place"
        for schema_value in matched_schema.values():
            _schema = schema_value.split("/")[0]
            if _schema != "Place":
                schema = _schema
        class_schema = getattr(module, schema)
        samples = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            sample_dict = dict()
            sample_dict["id"] = None
            for key, value in row_dict.items():
                if key in matched_schema:
                    root_col = matched_schema[key].split("/")[1]
                    sample_dict[root_col] = value
            sample = class_schema.from_dict(sample_dict)
            samples.append(sample)
        return samples

    def transform(self, df: pd.DataFrame, **kwargs):
        print("-"*40 + "SCHEMA MATCHING" + "-"*40)
        matched_schema = self.schema_matcher.transform(df)
        if not matched_schema:
            print(">> Schema not matched............")
            return
        print(">> Done!")
        print("-"*40 + "PROCESS DATA" + "-"*40)
        samples = self.get_sample_place_by_schema_matched(df, matched_schema)
        samples = self.process_samples(samples)
        print(">> Done!")
        print("-"*40 + "DATA MATCHING" + "-"*40)
        matched_data = self.data_matcher.transform(samples)
        print(">> Done!")
        print("-" * 40 + "DATA MAPPING" + "-" * 40)
        matched_data = self.data_mapper.transform(matched_data)
        return matched_data

    def matchingLocation(self, dataFrame):
        tmp = np.where(np.array(dataFrame.columns) == 'Longitude')[0]
        if tmp.size == 0:
            return dataFrame
        tmp = np.where(np.array(dataFrame.columns) == 'Latitude')[0]
        if tmp.size == 0:
            return dataFrame
        dictLocation = {}
        df = pd.DataFrame()
        listDict = []
        for _, row in dataFrame.iterrows():
            row_dict = row.to_dict()
            if (row_dict['Longitude'], row_dict['Latitude']) not in dictLocation.keys():
                dictLocation[(row_dict['Longitude'], row_dict['Latitude'])] = 1
                listDict.append(row_dict)
        df = df.append(listDict, ignore_index=True, sort=False)
        return df

    def fit(self, **kwargs):
        self.schema_matcher.fit()

    def save(self, dir_path, **kwargs):
        schema_matcher_path = os.path.join(dir_path, "schema_matcher")
        if os.path.exists(schema_matcher_path) is False:
            os.makedirs(schema_matcher_path)
        self.schema_matcher.save(schema_matcher_path)

    def load(self, dir_path, **kwargs):
        schema_matcher_path = os.path.join(dir_path, "schema_matcher")
        self.schema_matcher.save(schema_matcher_path)


