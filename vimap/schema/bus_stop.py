from vimap.schema.place import Place


class BusStop(Place):
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
            stop_type: str = None,
            street: str = None,
            **kwargs
    ):
        super(BusStop, self).__init__(
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
        self.stop_type = stop_type
        self.street = street