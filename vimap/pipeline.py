from vimap.extract.csv_extractor import CSVExtractor
from vimap.transform.transformer import Transformer
from vimap.load.mongo_loader import MongoLoader
from vimap.db_api import *


class Pipeline:
    def __init__(
            self,
            extractor=None,
            transformer=None,
            loader=None,
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

        if self.extractor is None:
            self.extractor = CSVExtractor()

        if self.transformer is None:
            self.transformer = Transformer()
            self.transformer.fit()

        if self.loader is None:
            self.loader = MongoLoader()

    def run(self, file_paths):
        for file in file_paths:
            df = self.extractor.load_csv(file)
            mapped_data = self.transformer.transform(df=df)
            self.loader.load_mongo(mapped_data)







