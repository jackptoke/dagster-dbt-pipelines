from typing import Literal


class Listing(object):
    def __init__(self,
                 listing_id: int,
                 title: str,
                 property_type: Literal[
                     "house", "apartment", "unit", "townhouse", "mansion", "complex", "building", "land"],
                 listing_type: Literal["rental", "buy", "sold"],
                 construction_status: Literal["new", "established", "under_construction"],
                 price: str,
                 advertised_price: str,
                 bedrooms: int,
                 bathrooms: int,
                 parking_spaces: int,
                 land_size: str,
                 description: str,
                 features: [str],
                 status: Literal["new", "sold"],
                 date_sold: str,
                 classic_project: bool,
                 agency_id: str,
                 agent_id: list[str],
                 address_id: str
                 ):
        self.listing_id = listing_id
        self.title = title
        self.property_type = property_type
        self.listing_type = listing_type
        self.construction_status = construction_status
        self.price = price
        self.advertised_price = advertised_price
        self.bedrooms = bedrooms
        self.bathrooms = bathrooms
        self.parking_spaces = parking_spaces
        self.land_size = land_size
        self.description = description
        self.features = features
        self.status = status
        self.date_sold = date_sold
        self.classic_project = classic_project
        self.agency_id = agency_id
        self.agent_id = agent_id
        self.address_id = address_id
