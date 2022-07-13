
class DataMapperBase:
    def __init__(self):
        pass

    def transform(self, matched_data, **kwargs):
        return matched_data

    def fit(self, **kwargs):
        pass

    def save(self, **kwargs):
        pass

    def load(self, **kwargs):
        pass

