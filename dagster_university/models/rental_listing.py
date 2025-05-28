from typing import Literal


class RentalListing(object):
    def __init__(self,
                 listing_id: int,
                 title: str,
                 property_type: Literal[
                     "house", "apartment", "unit", "townhouse", "mansion", "complex", "building", "land"],
                 listing_type: Literal["rental", "buy", "sold"],
                 price: str,
                 price_period: Literal["daily", "weekly", "monthly"],
                 bond: float,
                 bedrooms: int,
                 bathrooms: int,
                 parking_spaces: int,
                 description: str,
                 features: [str],
                 status: Literal["new", "sold"],
                 date_available: str,
                 classic_project: bool,
                 apply_online: bool,
                 agency_id: str,
                 agent_id: list[str],
                 address_id: str
                 ):
        self.listing_id = listing_id
        self.title = title
        self.property_type = property_type
        self.listing_type = listing_type
        self.price = price
        self.price_period = price_period
        self.bond = bond
        self.bedrooms = bedrooms
        self.bathrooms = bathrooms
        self.parking_spaces = parking_spaces
        self.description = description
        self.features = features
        self.status = status
        self.date_available = date_available
        self.classic_project = classic_project
        self.apply_online = apply_online
        self.agency_id = agency_id
        self.agent_id = agent_id
        self.address_id = address_id
