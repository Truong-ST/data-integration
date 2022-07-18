from vimap.schema.place import Place
from vimap.schema.school import School
from vimap.schema.bus_stop import BusStop
from vimap.schema.station import Station


schema_mapping_cols = {
    "longitude": "Place/longitude",
    "latitude": "Place/latitude",
    "easting": "Place/easting",
    "northing": "Place/northing",
    "address": "Place/address",
    "name": "Place/name",
    "telephone": "School/telephone",
    "website": "School/website",
    "postcode": "School/postcode",
    "stop_type": "BusStop/stop_type"
}