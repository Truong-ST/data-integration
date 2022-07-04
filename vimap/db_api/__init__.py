from vimap.db_api.constrains import *
from vimap.db_api.db_place_api import PlaceCollector
from vimap.db_api.db_school_api import SchoolCollector
from vimap.db_api.db_bus_stop_api import BusStopCollector
from vimap.db_api.db_station_api import StationCollector

place_collector = PlaceCollector()
school_collector = SchoolCollector()
bus_collector = BusStopCollector()
station_collector = StationCollector()

collectors = {
    PLACE_COLLECTION: place_collector,
    SCHOOL_COLLECTION: school_collector,
    BUS_STOP_COLLECTION: bus_collector,
    STATION_COLLECTION: station_collector
}