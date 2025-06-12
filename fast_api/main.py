import duckdb
import polars as pl

from fastapi import FastAPI, status, HTTPException
from scalar_fastapi import get_scalar_api_reference
from decorators import log
from models.listing import Listing
from typing import Literal
from datetime import datetime
from constants import LISTINGS_QUERY, SUBURBS_QUERY, STATES_QUERY, DUCKDB_FILE
from database import managed_db

app = FastAPI()
version = "v1"


@app.get(
    path=f"/api/{version}/listings",
    description="This endpoint returns all the listings that have been sold",
    tags=["listings", "sold"],
    status_code=status.HTTP_200_OK,
    response_model=list[Listing],
)
def get_sold_listing(channel: Literal["sold", "rent", "buy"] = "sold",
                     state: Literal["act", "nsw", "nt", "qld", "sa", "tas", "vic", "wa"] = "vic",
                     suburb: str = "Melbourne",
                     year: int = 2025) -> list[Listing]:
    log(f"get_sold_listing is requested")
    now = datetime.now()

    if year > now.year or year < (now.year-50):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Year must be between {(now.year-50)} and {now.year}"
        )

    with managed_db() as db:
        data_df = db.query(LISTINGS_QUERY, (state, suburb, year))
        print(f"Number of sold listings: {len(data_df)}")

    if len(data_df) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No {channel} properties found in {suburb}, {state} for the year {year}"
        )

    properties = [Listing(
        listing_id=data_df.item(index, "listing_id"),
        title=data_df.item(index, "title"),
        price=data_df.item(index, "price"),
        property_type=data_df.item(index, "property_type"),
        bedrooms=data_df.item(index, "bedrooms"),
        bathrooms=data_df.item(index, "bathrooms"),
        parking=data_df.item(index, "parking"),
        land=data_df.item(index, "land"),
        sold_date=data_df.item(index, "sold_date"),
        channel=data_df.item(index, "channel"),
        latitude=data_df.item(index, "latitude"),
        longitude=data_df.item(index, "longitude"),
        address=data_df.item(index, "address"),
        suburb=data_df.item(index, "suburb"),
        state=data_df.item(index, "state"),
        postcode=data_df.item(index, "postcode"),
    ) for index in range(len(data_df))]

    return properties


@app.get(f"/api/{version}/suburbs",
         tags=["listings", "suburbs"],
         response_model=list[str])
def get_available_suburbs(state: str = "vic") -> list[str]:
    with managed_db() as db:
        data_df = db.query(SUBURBS_QUERY, (state,))
    suburbs = [data_df.item(index, "suburb") for index in range(len(data_df))]
    return suburbs


@app.get(f"/api/{version}/states",
         tags=["listings", "states"],)
def get_available_states() -> list[str]:
    with managed_db() as db:
        data_df = db.query(STATES_QUERY)
    states = [data_df.item(index, "state") for index in range(len(data_df))]
    return states


@app.get("/scalar", include_in_schema=False)
def get_scalar_docs():
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title="Scalar API",
    )
