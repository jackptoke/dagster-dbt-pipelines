class Address(object):
    def __init__(self,
                 address_id: str,
                 street_address: str,
                 suburb: str,
                 state: str,
                 postcode: str,
                 locality: str,
                 subdivision_code: str,
                 latitude: float,
                 longitude: float
    ):
        self.address_id = address_id
        self.street_address = street_address
        self.suburb = suburb
        self.state = state
        self.postcode = postcode
        self.locality = locality
        self.subdivision_code = subdivision_code
        self.latitude = latitude
        self.longitude = longitude
