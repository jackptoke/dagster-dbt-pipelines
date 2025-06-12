from typing import Any
from pydantic import BaseModel
from enum import Enum
from datetime import date


class PropertyType(str, Enum):
    house = "house"
    unit = "unit"
    residential_land = "residential land"
    townhouse = "townhouse"
    other = "other"


class Channel(str, Enum):
    buy = "buy"
    sell = "sold"
    rent = "rent"


class AustralianState(str, Enum):
    act = "act"
    nsw = "nsw"
    nt = "nt"
    qld = "qld"
    sa = "sa"
    tas = "tas"
    vic = "vic"
    wa = "wa"


class Listing(BaseModel):
    listing_id: int
    title: str
    price: float
    property_type: PropertyType
    bedrooms: int
    bathrooms: int
    parking: int
    land: int
    sold_date: date
    channel: Channel
    latitude: float
    longitude: float
    address: str
    suburb: str
    state: AustralianState
    postcode: str
