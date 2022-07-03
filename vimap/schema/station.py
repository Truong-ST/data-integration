from vimap.schema.place import Place


class Station(Place):
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
            station_type: str = None,
            street: str = None,
            **kwargs
    ):
        super(Station, self).__init__(
            id=id,
            longitude=longitude,
            latitude=latitude,
            easting=easting,
            northing=northing,
            address=address,
            name=name,
            description=description,
            place_type=place_type,
            metadata=metadata
        )
        self.station_type = station_type
        self.street = street




