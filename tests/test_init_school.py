import pandas as pd
from uuid import uuid4
import json
from vimap.db_api.db_school_api import SchoolCollector

data = pd.read_csv("data/raw/schools-list.csv")
columns = data.columns
print(columns)

samples = []
school_samples = []
for i, row in data.iterrows():
    if i > 50:
        break
    row_dict = row.to_dict()
    id = str(uuid4())
    samples.append({
        "id": id,
        "longitude": row_dict.get("Longitude", None),
        "latitude": row_dict.get("Latitude", None),
        "easting": row_dict.get("Eastings", None),
        "northing": row_dict.get("Northings", None),
        "address": row_dict.get("Address 1", None),
        "name": row_dict.get("Establishment", None),
        "description": row_dict.get("longitude", None),
        "place_type": "School",
        "metadata": dict()
    })
    school_samples.append({
        "id": id,
        "longitude": row_dict.get("Longitude", None),
        "latitude": row_dict.get("Latitude", None),
        "head_name": row_dict.get("Head name", None),
        "telephone": row_dict.get("Telephone", None),
        "website": row_dict.get("Website", None),
        "postcode": row_dict.get("Postcode", None),
    })

place_df = pd.DataFrame(samples)
place_df.to_csv("data/samples/place_school.csv", index=False)


school_df = pd.DataFrame(school_samples)
school_df.to_csv("data/samples/school.csv", index=False)

collector = SchoolCollector()
collector.init_db_place_from_csv("data/samples/place_school.csv")
collector.init_db_school_from_csv("data/samples/school.csv")
