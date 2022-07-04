import pandas as pd
from uuid import uuid4
import json
from vimap.db_api.db_station_api import StationCollector
from vimap.utils.address_utils import convert_bng_to_latlon
data = pd.read_csv("data/raw/nir-rail-stations.csv")
columns = data.columns
print(columns)

samples = []
school_samples = []
for i, row in data.iterrows():
    if i > 50:
        break
    row_dict = row.to_dict()
    id = str(uuid4())
    easting = row_dict.get("EASTING", None)
    northing = row_dict.get("NORTHING", None)
    latlon = convert_bng_to_latlon(eastings=easting, northings=northing)
    longitude = latlon[0][0]
    latitude = latlon[1][0]
    address = row_dict.get("Address", None)
    name = row_dict.get("STATION", None)
    description = row_dict.get("description", None)
    samples.append({
        "id": id,
        "longitude": longitude,
        "latitude": latitude,
        "easting": easting,
        "northing": northing,
        "address": address,
        "name": name,
        "description": description,
        "place_type": "Station",
        "metadata": dict()
    })
    school_samples.append({
        "id": id,
        "longitude": longitude,
        "latitude": latitude,
        "station_type": row_dict.get("TYPE", None),
        "street": row_dict.get("Street", None)
    })

place_df = pd.DataFrame(samples)
place_df.to_csv("data/samples/place_station.csv", index=False)


school_df = pd.DataFrame(school_samples)
school_df.to_csv("data/samples/station.csv", index=False)

collector = StationCollector()

collector.init_db_place_from_csv("data/samples/place_station.csv")
collector.init_db_station_from_csv("data/samples/station.csv")