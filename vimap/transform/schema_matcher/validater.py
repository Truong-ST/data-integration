import pandas as pd


class Validater:
    def __init__(self):
        pass

    def _validate_postcode(self, col1, col2, df: pd.DataFrame=None):
        postcodes = df[col1]
        cnt = 0
        for value in postcodes:
            if len(value) > 0 and len(value) < 5:
                cnt += 1
        if cnt/len(postcodes) > 0.1:
            return False
        else:
            return True

    def _validate_head_name(self, col1, col2, df: pd.DataFrame=None):
        head_names = df[col1]
        cnt = 0
        for value in head_names:
            temp = value.split(' ')[0]
            if temp.lower() in ["mr", "mrs", "miss", "ms"]:
                cnt += 1
        if cnt / len(head_names) > 0.1:
            return True
        else:
            return False

    def _validate_address(self, col1, col2, df: pd.DataFrame=None):
        addresses = df[col1]
        cnt = 0
        for value in addresses:
            temp = value.split(' ')[0]
            if len(temp) <= 1:
                cnt += 1
        if cnt / len(addresses) > 0.3:
            return False
        else:
            return True

    def validate(self, col1, col2, df: pd.DataFrame=None):
        try:
            func_validate = getattr(self, f'_validate_{col2}')
        except:
            print(col1)
            return False
        if func_validate:
            return func_validate(col1, col2, df)

        return True