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
            data_matcher: SchemaMatcherBase = None,
            data_mapper: DataMatcherBase = None,
            **kwargs
    ):
        self.schema_matcher = schema_matcher or SchemaMatcher()
        self.data_matcher = data_matcher
        self.data_mapper = data_mapper
        self.schema_list = []

    def transform(self, df: pd.DataFrame, **kwargs):
        matched_schema = self.schema_matcher.transform(df)
        if not matched_schema:
            return
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


