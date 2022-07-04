from vimap.extract.csv_extractor import CSVExtractor
from vimap.transform.transformer import Transformer
from vimap.load.mongo_loader import Loader
from vimap.db_api import *
extractor = CSVExtractor()
transformer = Transformer()
loader = Loader()

transformer.fit()

## Run

file_paths = [
    "data/raw/translink-stationsni.csv"
]

for file in file_paths:
    df = extractor._load_csv(file)
    samples = transformer.transform(df=df)
    loader.load(samples)




