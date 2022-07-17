import os
from typing import List, Text, Union, Dict
import flexmatcher
from flexmatcher import FlexMatcher
import numpy as np
import statistics
from statistics import mode
import pandas as pd
from pandas import DataFrame
import numpy as np
import pandas as pd
import Levenshtein as Lv

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
            col_value_leven_thresh: float = 0.7,
            flexmatcher_sample_size: int = 100,
            matched_by_names_schema: list = [],
            string_mapper=None,
            validater=None,
            use_flexmatcher=False,
            **kwargs
    ):
        super(SchemaMatcher, self).__init__(**kwargs)
        self.col_name_score_thresh = col_name_score_thresh
        self.col_value_score_thresh = col_value_score_thresh
        self.col_value_leven_thresh = col_value_leven_thresh
        self.schemas = schemas if schemas else {}

        self.matched_by_names_schema = matched_by_names_schema

        self.use_flexmatcher = use_flexmatcher
        if self.use_flexmatcher:
            self.flexmatcher = self._create_flexmatcher(flexmatcher_sample_size)
        else:
            self.flexmatcher = None

        self.flexmatcher_sample_size = flexmatcher_sample_size
        self.string_mapper = string_mapper
        self.validater = validater or Validater()
        if self.string_mapper is None:
            self.string_mapper = StringMapper()
            self.string_mapper.load("configs/col_name_mappings.json")

    def load_schema_sample(self):
        pass

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

    def _load_schema_samples(self, limit=None):
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

            if not scores:
                outputs[ex_col] = None
                continue

            # print(f"{ex_col} ________ {scores}")
            top_1_score = scores[0][1]
            if top_1_score >= self.col_name_score_thresh:
                outputs[ex_col] = {"schema": scores[0][0], "score": top_1_score}
            else:
                outputs[ex_col] = None
        return outputs

    @staticmethod
    def __get_set_combinations(
            set1: set,
            set2: set,
            threshold: float):
        for s1 in set1:
            yield str(s1), set2, threshold

    @staticmethod
    def __process_lv(tup: tuple):
        """
        Function that check if there exist entry from the second set that has a greater Levenshtein ratio with the
        element from the first set than the given threshold
        Parameters
        ----------
        tup : tuple
            A tuple containing one element from the first set, the second set and the threshold of the Levenshtein ratio
        Returns
        -------
        int
            1 if there is such an element 0 if not
        """
        s1, set2, threshold = tup
        for s2 in set2:
            if Lv.ratio(s1, str(s2)) >= threshold:
                return 1
        return 0

    def _get_score_of_col_value(self, values_col1, values_cl2, threshold=0.85):
        col1_types = [type(x) for x in values_col1]
        col2_types = [type(x) for x in values_cl2]

        if mode(col1_types) != mode(col2_types):
            return 0.0
        values_col1 = set(values_col1)
        values_cl2 = set(values_cl2)

        if len(set(values_col1)) < len(set(values_cl2)):
            set1 = set(values_col1)
            set2 = set(values_cl2)
        else:
            set1 = set(values_cl2)
            set2 = set(values_col1)

        combinations = self.__get_set_combinations(set1, set2, threshold)
        intersection_cnt = 0
        for cmb in combinations:
            intersection_cnt = intersection_cnt + self.__process_lv(cmb)

        union_cnt = len(set1) + len(set2) - intersection_cnt
        if union_cnt == 0:
            sim = 0.0
        else:
            sim = float(intersection_cnt) / union_cnt

        return sim

    def matcher_col_value(self, external_df: DataFrame):
        outputs = {}
        for ex_col in external_df.columns:
            scores = {}
            value_col1 = list(external_df[ex_col])
            for schema_name, df in self.schemas.items():
                for col in df.columns:
                    value_col2 = list(df[col])
                    scores[f"{schema_name}/{col}"] = self._get_score_of_col_value(value_col1, value_col2,
                                                                                  threshold=self.col_value_leven_thresh)
            scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
            if not scores:
                outputs[ex_col] = None
                continue
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
        if self.flexmatcher is not None:
            self.flexmatcher.save_model(flexmatcher_model_path)

        # save config
        config_path = os.path.join(matcher_dir, "config.json")
        write_json(config, config_path)

        # save schema
        sample_schema_path = os.path.join(matcher_dir, "sample_schema")
        if os.path.exists(sample_schema_path) is False:
            os.makedirs(sample_schema_path)
        for name, df in self.schemas.items():
            df_path = os.path.join(sample_schema_path, f'{name}.csv')
            df.to_csv(df_path, index=False)

    def load(self, dir_path, **kwargs):
        matcher_dir = os.path.join(dir_path, self.__class__.__name__)

        # load flexmatcher
        flexmatcher_model_path = os.path.join(matcher_dir, self.flexmatcher.__class__.__name__ + ".bin")
        if os.path.exists(flexmatcher_model_path) and self.flexmatcher is not None:
            self.flexmatcher.load_model(flexmatcher_model_path)

        # load config
        config_path = os.path.join(matcher_dir, "config.json")
        config = load_json(config_path)
        for key, value in config.items():
            setattr(self, key, value)

        # load schema
        sample_schema_path = os.path.join(matcher_dir, "sample_schema")
        if os.path.exists(sample_schema_path):
            file_names = os.listdir(sample_schema_path)
            for file_name in file_names:
                df_path = os.path.join(sample_schema_path, file_name)
                if file_name.endswith(".csv"):
                    self.schemas[file_name.strip('.csv')] = pd.read_csv(df_path)

        if not self.schemas:
            self.schemas = self._load_schema_samples()

    def fit(self, **kwargs):
        if self.flexmatcher and self.use_flexmatcher:
            self.flexmatcher.train()

    def _validate(self, matcher_output, df=None):
        _validated_output = {}
        for key, value in matcher_output.items():
            if self.validater.validate(key, value, df):
                _validated_output[key] = value

        return _validated_output

    def transform(self, external_df: DataFrame, **kwargs):
        by_col_name_output = self.matcher_col_name(external_df)
        print(f">> Schema matching by column name: {by_col_name_output}")

        final_output = {}
        for col, schema in by_col_name_output.items():
            if schema:
                final_output[col] = schema["schema"]

        matched_dop_cols = []
        for col in final_output:
            matched_dop_cols.append(col)

        external_df = external_df.drop(matched_dop_cols, axis=1)

        by_col_value_output = self.matcher_col_value(external_df)
        print(f">> Schema matching by column value: {by_col_value_output}")

        for col, schema in by_col_value_output.items():
            if schema:
                final_output[col] = schema["schema"]

        external_df = self._process_df(external_df)
        non_text_cols = []
        for col in external_df.columns:
            if external_df.dtypes[col] == np.int64 or external_df.dtypes[col] == np.float64:
                non_text_cols.append(col)

        external_df = external_df.drop(non_text_cols, axis=1)
        if self.use_flexmatcher and self.flexmatcher:
            flex_output = self.flexmatcher.make_prediction(external_df)
            print(f">> Schema matching by flexmatcher: {flex_output}")
        else:
            flex_output = {}

        final_output.update(flex_output)
        final_output = self._validate(final_output, df=external_df)
        return final_output


# external_df = pd.read_csv("data/raw/translink-stationsni.csv")
#
# matched_col_name_schema = [
#     "Place/easting",
#     "Place/northing",
#     "Place/longitude"
#     "Place/latitude"
# ]
#
# string_mapper = StringMapper()
# string_mapper.load("configs/col_name_mappings.json")
#
# matcher = SchemaMatcher(
#     matched_by_names_schema=matched_col_name_schema,
#     string_mapper=string_mapper,
#     use_flexmatcher=False
# )

# output = matcher.matcher_col_name(external_df)
# output = matcher.matcher_col_value(external_df)

# matcher.fit()
# matcher.load("models/schema_matcher")
# matcher.save("models/schema_matcher")
# output = matcher.transform(external_df)
# print(output)