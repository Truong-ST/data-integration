from typing import List, Text, Union, Dict
from vimap.utils.address_utils import *


class Place:
    def __init__(
            self,
            id,
            longitude: float = None,
            latitude: float = None,
            easting: float = None,
            northing: float = None,
            address: str = None,
            name: str = None,
            description: str = None,
            place_type: str = None,
            metadata: dict = None,
            **kwargs
    ):
        self.id = id
        self.longitude = longitude
        self.latitude = latitude
        self.easting = easting
        self.northing = northing
        self.address = address
        self.name = name
        self.description = description
        self.place_type = place_type
        self.metadata = metadata or {}

        if self.easting is None or self.northing is None:
            self._update_latlon(self.longitude, self.latitude)
        if self.longitude is None or self.latitude is None:
            self._update_bng(self.easting, self.northing)

    def to_dict(self):
        return self.place_info()

    def place_info(self):
        place = dict()
        place["id"] = self.id
        place["longitude"] = self.longitude
        place["latitude"] = self.latitude
        place["easting"] = self.easting
        place["northing"] = self.northing
        place["address"] = self.address
        place["name"] = self.name
        place["description"] = self.description
        place["place_type"] = self.place_type
        place["metadata"] = self.metadata
        return place

    @classmethod
    def from_dict(cls, data, **kwargs):
        return cls(**data, **kwargs)

    def _update_latlon(
            self,
            longitude: float = None,
            latitude: float = None,
    ):
        if longitude is None or latitude is None:
            return
        self.longitude = longitude
        self.latitude = latitude
        bng_coors = convert_lonlat_to_bng(longitude, latitude)
        self.easting = bng_coors[0][0]
        self.northing = bng_coors[1][0]

    def _update_bng(self, easting, northing):
        if easting is None or northing is None:
            return
        self.easting = easting
        self.northing = northing
        latlon_coors = convert_bng_to_latlon(easting, northing)
        self.longitude = latlon_coors[0][0]
        self.latitude = latlon_coors[1][0]

