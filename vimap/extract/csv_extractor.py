import pandas as pd
from typing import List, Union, Dict, Text


class CSVEtractor:
    def __init__(self):
        self.samples = []

    def _load_csv(self, file_paths):
        for file in file_paths:
            _samples = []
            try:
                df = pd.read_csv(file, encoding="ISO 8859-1")
                for _, row in df.iterrows():
                    _samples.append(row.to_dict())
                yield _samples
            except:
                raise Exception(f"Fail on file: {file}")
