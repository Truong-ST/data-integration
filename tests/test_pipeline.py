from vimap.extract.csv_extractor import CSVEtractor


extractor = CSVEtractor()







## Run

file_paths = [
    "data/schools-list.csv",
    "data/09-05-2022busstop-list.csv"
]

datasets = extractor._load_csv(file_paths)
for data in datasets:
    print(data)
