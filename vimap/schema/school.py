from vimap.schema.place import Place


class School(Place):
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
            head_name: str = None,
            telephone: str = None,
            website: str = None,
            postcode: str = None,
            phase: str = None,
            **kwargs
    ):
        super(School, self).__init__(
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
        self.head_name = head_name
        self.telephone = telephone
        self.website = website
        self.postcode = postcode
        self.phase = phase

    def school_info(self):
        school = dict()
        school["head_name"] = self.head_name
        school["telephone"] = self.telephone
        school["website"] = self.website
        school["postcode"] = self.postcode
        school["phase"] = self.phase
        return school


