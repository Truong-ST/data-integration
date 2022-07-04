import pandas as pd
from uuid import uuid4
import json
from vimap.db_api.db_bus_stop_api import BusStopCollector
from vimap.utils.address_utils import convert_bng_to_latlon
data = pd.read_csv("data/raw/09-05-2022busstop-list.csv", encoding='cp1252')
columns = data.columns
print(columns)

samples = []
bus_samples = []
for i, row in data.iterrows():
    if i > 50:
        break
    row_dict = row.to_dict()
    id = str(uuid4())
    longitude = row_dict.get("Longitude", None)
    latitude = row_dict.get("Latitude", None)
    easting = row_dict.get("Easting", None)
    northing = row_dict.get("Northing", None)
    address = row_dict.get("Address", None)
    name = row_dict.get("CommonName", None)
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
        "place_type": "BusStop",
        "metadata": dict()
    })
    bus_samples.append({
        "id": id,
        "longitude": longitude,
        "latitude": latitude,
        "stop_type": row_dict.get("StopType", None),
        "street": row_dict.get("Street", None),
        "locality": row_dict.get("LocalityName", None)
    })

place_df = pd.DataFrame(samples)
place_df.to_csv("data/samples/place_bus.csv", index=False)


bus_df = pd.DataFrame(bus_samples)
bus_df.to_csv("data/samples/bus.csv", index=False)

collector = BusStopCollector()

collector.init_db_place_from_csv("data/samples/place_bus.csv")
collector.init_db_bus_stop_from_csv("data/samples/bus.csv")