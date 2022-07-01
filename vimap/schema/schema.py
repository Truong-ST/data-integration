from typing import List, Text, Union, Dict


class Place:
    def __init__(
            self,
            longitude=None,
            latitude=None,
            **kwargs
    ):
        self.longitude = longitude
        self.latitude = latitude


class Hotel(Place):
    def __init__(
            self,
            longitude=None,
            latitude=None
    ):
        super(Hotel, self).__init__()
