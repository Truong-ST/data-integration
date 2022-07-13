import pandas as pd
from typing import List, Union, Dict, Text


class CSVExtractor:
    def __init__(self):
        self.samples = []

    def load_csv(self, file_path):
        try:
            df = pd.read_csv(file_path, encoding="ISO 8859-1")
            return df
        except:
            raise Exception(f"Fail on file: {file_path}")
