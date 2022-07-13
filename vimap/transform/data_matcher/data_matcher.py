import os
from typing import List, Text, Union, Dict
from collections import defaultdict
import numpy as np
import statistics
from statistics import mode
import pandas as pd
from pandas import DataFrame
import Levenshtein as Lv

from vimap.transform.data_matcher.data_matcher_base import DataMatcherBase
from vimap.utils.string_utils import *
from vimap.utils.file_utils import *
from vimap.db_api import collectors
from vimap.db_api.constrains import PLACE_COLLECTION
from vimap.schema import schema_mapping_cols
from vimap.schema import *


class DataMatcher(DataMatcherBase):
    def __init__(
            self,
            string_mapper=None,
            place_name_score_thresh=0.9
    ):
        super(DataMatcher, self).__init__()
        self.place_name_score_thresh = place_name_score_thresh
        self.places_dict = self._load_places()

    def _get_all_place(self):
        places = []
        for key, values in self.places_dict.items():
            places.extend(values)

        return places

    def _load_places(self):
        places_dict = defaultdict(list)
        places = collectors[PLACE_COLLECTION].get()
        for place in places:
            places_dict[place.get("place_type", "default")].append(place)

        return places_dict

    def _matching_by_latlon(self, sample: Place, **kwargs):
        assert isinstance(sample, Place), f"{sample}"
        if sample.latitude and sample.longitude:
            filter_sample = collectors[PLACE_COLLECTION].find_by_latlon(
                longitude=sample.longitude,
                latitude=sample.latitude
            )
            if filter_sample:
                return filter_sample["id"]
        return None

    def _matching_by_name(self, sample: Place, places: List[Dict], **kwargs):
        scores = {}
        for place in places:
            place_name = place.get("name", "")
            scores[place["id"]] = levenshtein_score(sample.name, place_name)
        scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)

        if not scores:
            return None
        top_1_score = scores[0][1]
        if top_1_score >= self.place_name_score_thresh:
            return scores[0][0]
        return None

    def matching_sample(self, sample: Place, **kwargs):
        matched_sample_id = self._matching_by_latlon(sample)
        if matched_sample_id:
            return matched_sample_id

        places = []
        if sample.place_type:
            places = self.places_dict[sample.place_type]

        if not places:
            places = self._get_all_place()

        matched_sample_id = self._matching_by_name(sample, places)

        if matched_sample_id:
            return matched_sample_id
        return None

    def transform(self, samples: List[Place], **kwargs):
        matched_data = []
        for sample in samples:
            matched_sample_id = self.matching_sample(sample)
            matched_data.append([sample, matched_sample_id])

        return matched_data

    def fit(self, **kwargs):
        pass

    def save(self, **kwargs):
        pass

    def load(self, **kwargs):
        pass