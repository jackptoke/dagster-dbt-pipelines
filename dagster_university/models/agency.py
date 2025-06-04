class Agency(object):
    def __init__(self,
                 agency_id: int,
                 name: str,
                 email: str,
                 address_id: str,
                 website: str,
                 phone_number: str
                 ):
        self.agency_id = agency_id
        self.name = name
        self.email = email
        self.address_id = address_id
        self.website = website
        self.phone_number = phone_number
