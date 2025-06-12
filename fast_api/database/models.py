from sqlmodel import SQLModel


class Listing(SQLModel):
    listing_id: int
    title: str
    price: float
    property_type: str
    bedrooms: int
    bathrooms: int
    parking: int
    land: int
    sold_date: str
    channel: str
    latitude: float
    longitude: float
    address: str
    suburb: str
    state: str
    postcode: str
