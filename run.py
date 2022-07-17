from vimap.pipeline import Pipeline
from vimap.extract.csv_extractor import CSVExtractor
from vimap.transform.transformer import Transformer
from vimap.load.mongo_loader import MongoLoader
from vimap.transform.data_mapper import *
from vimap.utils.string_utils import *
from vimap.transform.data_matcher import *
from vimap.transform.schema_mapper import *
from vimap.transform.schema_matcher import *
from vimap.transform.schema_matcher.schema_matcher import SchemaMatcher
from vimap.db_api import *


if __name__ == '__main__':
    file_paths = [
        "data/raw/09-05-2022busstop-list.csv",
        "data/raw/TfGMStoppingPoints.csv",
        "data/raw/schools-list.csv"
    ]
    # create extractor
    extractor = CSVExtractor()

    # create transformer
    string_mapper = StringMapper()
    string_mapper.load(ontology_path="configs/col_name_mappings.json")
    schema_matcher = SchemaMatcher(
        use_flexmatcher=False,
        string_mapper=string_mapper
    )
    data_matcher = DataMatcher()
    data_mapper = DataMapper()
    transformer = Transformer(
        schema_matcher=schema_matcher,
        data_matcher=data_matcher,
        data_mapper=data_mapper
    )

    # create loader
    loader = MongoLoader()
    pipline = Pipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader
    )
    pipline.run(file_paths)
