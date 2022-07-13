from vimap.pipeline import Pipeline
from vimap.extract.csv_extractor import CSVExtractor
from vimap.transform.transformer import Transformer
from vimap.load.mongo_loader import MongoLoader
from vimap.transform.data_mapper import *
from vimap.transform.data_matcher import *
from vimap.transform.schema_mapper import *
from vimap.transform.schema_matcher import *
from vimap.transform.schema_matcher.schema_matcher import SchemaMatcher
from vimap.db_api import *


if __name__ == '__main__':
    file_paths = [
        # "data/raw/nir-rail-stations.csv",
        "data/raw/translink-stationsni.csv"
    ]
    extractor = CSVExtractor()
    pipline = Pipeline()
    pipline.run(file_paths)