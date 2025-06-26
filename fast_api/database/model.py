from sqlalchemy import Column, Integer, String, Date, Float
from .session import Base

# def id_field(table_name: str):
#     sequence = Sequence(f"{table_name}_id_seq")
#     return Field(
#         default=None,
#         primary_key=True,
#         sa_column_args=[sequence],
#         sa_column_kwargs={"server_default": sequence.next_value()},
#     )


class Listing(Base):
    __tablename__ = "fct_listing"
    listing_id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(Integer)
    property_type = Column(String)
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    parking = Column(Integer)
    land = Column(Integer)
    sold_date = Column(Date)
    year = Column(Integer)
    channel = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    address = Column(String)
    suburb = Column(String)
    state = Column(String)
    postcode = Column(String)

