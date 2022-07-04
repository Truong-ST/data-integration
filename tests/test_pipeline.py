from vimap.extract.csv_extractor import CSVExtractor


extractor = CSVExtractor()







## Run

file_paths = [
    "data_matcher/schools-list.csv",
    "data_matcher/09-05-2022busstop-list.csv"
]

datasets = extractor._load_csv(file_paths)
for data in datasets:
    print(data)
