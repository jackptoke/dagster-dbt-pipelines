from datetime import datetime
from typing import Literal

from fastapi import FastAPI, status, HTTPException
from fastapi.params import Depends
from sqlalchemy.orm import Session
from scalar_fastapi import get_scalar_api_reference
from sqlalchemy import select, text

from constants import SUBURBS_QUERY, STATES_QUERY, LISTINGS_QUERY
from database import managed_db
from database import model
from database.session import db_engine, get_db
from decorators import log
from models.listing import Listing


app = FastAPI() # lifespan_handler=lifespan_handler
version = "v1"

model.Base.metadata.create_all(bind=db_engine)


@app.get(
    path=f"/api/{version}/listings",
    description="This endpoint returns all the listings that have been sold",
    tags=["listings", "sold"],
    status_code=status.HTTP_200_OK,
    response_model=list[Listing],
)
def get_sold_listing(db_session: Session = Depends(get_db),
                     channel: Literal["sold", "rent", "buy"] = "sold",
                     state: Literal["act", "nsw", "nt", "qld", "sa", "tas", "vic", "wa"] = "vic",
                     suburb: str = "Melbourne",
                     year: int = 2025,
                     ):
    log(f"get_sold_listing is requested")
    now = datetime.now()

    if year > now.year or year < (now.year-50):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Year must be between {(now.year-50)} and {now.year}"
        )

    listings = db_session.query(model.Listing).filter_by(
        year=year, suburb=suburb.lower(), state=state.lower(),
        channel=channel.lower()).all()

    # data_df = session.exec(LISTINGS_QUERY, params={"state": state, "suburb": suburb, "year": year})
    # with managed_db() as db:
    #     data_df = db.query(LISTINGS_QUERY, (state, suburb, year))
    #     print(f"Number of sold listings: {len(data_df)}")

    if len(listings) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No {channel} properties found in {suburb}, {state} for the year {year}"
        )

    return listings
    # properties = [Listing(
    #     listing_id=listings[index]["listing_id"],
    #     title=listings[index]["title"],
    #     price=listings[index][index, "price"],
    #     property_type=listings[index][index, "property_type"],
    #     bedrooms=listings[index][index, "bedrooms"],
    #     bathrooms=listings[index][index, "bathrooms"],
    #     parking=listings[index][index, "parking"],
    #     land=listings[index][index, "land"],
    #     sold_date=listings[index][index, "sold_date"],
    #     channel=listings[index][index, "channel"],
    #     latitude=listings[index][index, "latitude"],
    #     longitude=listings[index][index, "longitude"],
    #     address=listings[index][index, "address"],
    #     suburb=listings[index][index, "suburb"],
    #     state=listings[index][index, "state"],
    #     postcode=listings[index][index, "postcode"],
    # ) for index in range(len(listings))]
    #
    # return properties


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
