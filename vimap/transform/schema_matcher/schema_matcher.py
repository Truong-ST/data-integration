import os
from typing import List, Text, Union, Dict
import flexmatcher
from flexmatcher import FlexMatcher
import numpy as np
import statistics
from statistics import mode
import pandas as pd
from pandas import DataFrame

from vimap.transform.schema_matcher.schema_matcher_base import SchemaMatcherBase
from vimap.utils.string_utils import *
from vimap.utils.file_utils import *
from vimap.db_api import collectors
from vimap.db_api.constrains import PLACE_COLLECTION
from vimap.schema import schema_mapping_cols
from vimap.transform.schema_matcher.validater import Validater


class SchemaMatcher(SchemaMatcherBase):
    def __init__(
            self,
            schemas: Dict[Text, DataFrame] = None,
            col_name_score_thresh: float = 0.9,
            col_value_score_thresh: float = 0.6,
            flexmatcher_sample_size: int = 5,
            matched_by_names_schema: list = [],
            string_mapper=None,
            validater=None,
            **kwargs
    ):
        super(SchemaMatcher, self).__init__(**kwargs)
        self.col_name_score_thresh = col_name_score_thresh
        self.col_value_score_thresh = col_value_score_thresh
        self.schemas = schemas or self._load_schema_samples()
        self.matched_by_names_schema = matched_by_names_schema
        self.flexmatcher = self._create_flexmatcher(flexmatcher_sample_size)
        self.flexmatcher_sample_size = flexmatcher_sample_size
        self.string_mapper = string_mapper
        self.validater = validater or Validater()
        if self.string_mapper is None:
            self.string_mapper = StringMapper()
            self.string_mapper.load("configs/col_name_mappings.json")

    def _process_df(self, df):
        df = df.dropna(how='all', axis=1)
        df = df.replace(r'^\s*$', np.nan, regex=True).dropna(axis=0)
        return df

    def _create_flexmatcher(self, sample_size=100):
        schema_list = []
        for coll_name, collector in collectors.items():
            samples = collector.get(limit=sample_size)
            df = pd.DataFrame(samples)
            df = df.drop_duplicates()
            df = df.drop(['id', '_id'], axis=1)
            if "place_type" in df.columns:
                df = df.drop(['place_type', 'metadata'], axis=1)

            df = self._process_df(df)
            schema_list.append(df)
        for i, df in enumerate(schema_list):
            columns = df.columns
            non_text_cols = []
            for col in columns:
                if df.dtypes[col] == np.int64 or df.dtypes[col] == np.float64:
                    non_text_cols.append(col)
            df = df.drop(non_text_cols, axis=1)
            schema_list[i] = df

        mapping_list = [dict(zip(df.columns, df.columns)) for df in schema_list]
        fm = flexmatcher.FlexMatcher(schema_list, mapping_list, sample_size=sample_size)
        return fm

    def _load_schema_samples(self, limit=100):
        schemas = {}
        for coll_name, collector in collectors.items():
            samples = collector.get(limit=limit)
            df = pd.DataFrame(samples)
            df = df.drop_duplicates()
            df = df.drop(['id', '_id'], axis=1)
            if coll_name == PLACE_COLLECTION:
                df = df.drop(['place_type', "metadata"], axis=1)
            if coll_name != PLACE_COLLECTION:
                df = df.drop(['longitude', 'latitude'], axis=1)

            df = self._process_df(df)
            schemas[coll_name] = df
        return schemas

    def _get_score_of_col_name(self, col1, col2):
        score = levenshtein_score(col1, col2)
        return score

    def matcher_col_name(self, external_df: DataFrame):
        outputs = {}
        for ex_col in external_df.columns:
            mapping_col, mapping_score = self.string_mapper.map(ex_col)
            if mapping_score >= self.col_name_score_thresh:
                outputs[ex_col] = {"schema": schema_mapping_cols[mapping_col], "score": mapping_score}
                continue
            scores = {}
            for schema_name, df in self.schemas.items():
                for col in df.columns:
                    col1 = str(ex_col).lower()
                    col2 = str(col).lower()
                    scores[f"{schema_name}/{col}"] = self._get_score_of_col_name(col1, col2)

            scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
            top_1_score = scores[0][1]
            if top_1_score >= self.col_name_score_thresh:
                outputs[ex_col] = {"schema": scores[0][0], "score": top_1_score}
            else:
                outputs[ex_col] = None
        return outputs

    def _get_score_of_col_value(self, values_col1, values_cl2):
        col1_types = [type(x) for x in values_col1]
        col2_types = [type(x) for x in values_cl2]

        if mode(col1_types) != mode(col2_types):
            return 0.0
        values_col1 = set(values_col1)
        values_cl2 = set(values_cl2)
        intersection_size = len(values_cl2.intersection(values_col1))
        union_size = len(values_cl2.union(values_col1))
        jaccard_score = intersection_size / union_size
        return jaccard_score

    def matcher_col_value(self, external_df: DataFrame):
        outputs = {}
        for ex_col in external_df.columns:
            scores = {}
            value_col1 = list(external_df[ex_col])
            for schema_name, df in self.schemas.items():
                for col in df.columns:
                    value_col2 = list(df[col])
                    scores[f"{schema_name}/{col}"] = self._get_score_of_col_value(value_col1, value_col2)
            scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
            top_1_score = scores[0][1]
            if top_1_score >= self.col_value_score_thresh:
                outputs[ex_col] = {"schema": scores[0][0], "score": top_1_score}
            else:
                outputs[ex_col] = None
        return outputs

    def save(self, dir_path, **kwargs):
        matcher_dir = os.path.join(dir_path, self.__class__.__name__)
        if os.path.exists(matcher_dir) is False:
            os.makedirs(matcher_dir)

        config = {
            "name": self.__class__.__name__,
            "col_name_score_thresh": self.col_name_score_thresh
        }

        # save flexmatcher
        flexmatcher_model_path = os.path.join(matcher_dir, self.flexmatcher.__class__.__name__ + ".bin")
        self.flexmatcher.save_model(flexmatcher_model_path)

        # save config
        config_path = os.path.join(matcher_dir, "config.json")
        write_json(config, config_path)

    def load(self, dir_path, **kwargs):
        matcher_dir = os.path.join(dir_path, self.__class__.__name__)

        # load flexmatcher
        flexmatcher_model_path = os.path.join(matcher_dir, self.flexmatcher.__class__.__name__ + ".bin")
        self.flexmatcher.load_model(flexmatcher_model_path)

        # load config
        config_path = os.path.join(matcher_dir, "config.json")
        config = load_json(config_path)
        for key, value in config.items():
            setattr(self, key, value)

    def fit(self, **kwargs):
        self.flexmatcher = self._create_flexmatcher(self.flexmatcher_sample_size)
        self.flexmatcher.train()

    def _validate(self, matcher_output, df=None):
        _validated_output = {}
        for key, value in matcher_output.items():
            if self.validater.validate(key, value, df):
                _validated_output[key] = value

        return _validated_output

    def transform(self, external_df: DataFrame, **kwargs):
        # matcher by col name
        by_col_name_output = self.matcher_col_name(external_df)
        final_output = {}
        for col, schema in by_col_name_output.items():
            if schema:
                final_output[col] = schema["schema"]

        matched_dop_cols = []
        for col in final_output:
            matched_dop_cols.append(col)

        external_df = external_df.drop(matched_dop_cols, axis=1)

        by_col_value_output = self.matcher_col_value(external_df)
        for col, schema in by_col_value_output.items():
            if schema:
                final_output[col] = schema["schema"]

        external_df = self._process_df(external_df)
        non_text_cols = []
        for col in external_df.columns:
            if external_df.dtypes[col] == np.int64 or external_df.dtypes[col] == np.float64:
                non_text_cols.append(col)
        external_df = external_df.drop(non_text_cols, axis=1)
        flex_output = self.flexmatcher.make_prediction(external_df)
        flex_output = self._validate(flex_output, df=external_df)
        final_output.update(flex_output)
        return final_output

#
# external_df = pd.read_csv("data/raw/translink-stationsni.csv")
#
# matched_col_name_schema = [
#     "Place/easting",
#     "Place/northing",
#     "Place/longitude"
#     "Place/latitude"
# ]
# matcher = SchemaMatcher(
#     matched_by_names_schema=matched_col_name_schema
# )
#
# # output = matcher.matcher_col_name(external_df)
# # output = matcher.matcher_col_value(external_df)
#
# matcher.fit()
# # matcher.load("models/schema_matcher")
# # matcher.save("models/schema_matcher")
# output = matcher.transform(external_df)
# print(output)