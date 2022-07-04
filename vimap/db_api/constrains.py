import os
from dotenv import load_dotenv
load_dotenv()

MONGO_DB = os.environ["MONGO_DB"]
MONGO_URI = os.environ['MONGO_URI']

PLACE_COLLECTION = "Place"
SCHOOL_COLLECTION = 'School'
STATION_COLLECTION = 'Station'
BUS_STOP_COLLECTION = 'BusStop'

COLLECTIONS = [
    PLACE_COLLECTION,
    SCHOOL_COLLECTION,
    STATION_COLLECTION,
    BUS_STOP_COLLECTION
]