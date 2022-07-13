import os
from typing import List, Text, Union, Dict
import flexmatcher
from flexmatcher import FlexMatcher
import numpy as np
import statistics
from statistics import mode
import pandas as pd
from pandas import DataFrame
from uuid import uuid4

from vimap.transform.data_mapper.data_mapper_base import DataMapperBase
from vimap.utils.string_utils import *
from vimap.utils.file_utils import *
from vimap.db_api import collectors
from vimap.db_api.constrains import PLACE_COLLECTION
from vimap.schema import schema_mapping_cols
from vimap.schema import *


class DataMapper(DataMapperBase):
    def __init__(
            self
    ):
        super(DataMapper, self).__init__()

    def transform(self, matched_data: List, **kwargs):
        for i, (sample, matched_id) in enumerate(matched_data):
            if matched_id is None:
                matched_data[i][0].id = str(uuid4())
        return matched_data

    def fit(self, **kwargs):
        pass

    def save(self, **kwargs):
        pass

    def load(self, **kwargs):
        pass