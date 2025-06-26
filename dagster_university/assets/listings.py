import asyncio
import json
import math
import os
import re
from datetime import datetime

import aiohttp
import dagster as dg
import polars as pl
import tenacity
from dagster import asset, AssetOut, op, Out, AutomationCondition, multi_asset, AssetExecutionContext, Output
from dagster_aws.s3 import S3Resource
from smart_open import open

from dagster_university.assets import constants
from dagster_university.assets.listings_support import find_address, find_agent, find_agency, \
    get_ad_price, get_surface_area
from dagster_university.models.address import Address
from dagster_university.models.agency import Agency
from dagster_university.models.agent import Agent
from dagster_university.models.listing import Listing
from dagster_university.models.rental_listing import RentalListing
from dagster_university.partitions import suburb_channel_partitions
from ..resources import smart_open_config


@tenacity.retry(stop=tenacity.stop_after_attempt(5),
                wait=tenacity.wait_fixed(3),
                reraise=True)
async def fetch(session, url, params, headers) -> dict | aiohttp.ClientResponse:
    async with session.get(url=url, params=params, headers=headers) as response:
        assert response.status == 200
        return await response.json()


@asset(partitions_def=suburb_channel_partitions,
       metadata={"partition_expr": {"channel": "channel", "suburb": "suburb"}},
       group_name="downloaded",
       # retry_policy=RetryPolicy(max_retries=5, delay=60, backoff=Backoff(Backoff.EXPONENTIAL)),
       compute_kind="Python",
       code_version="v1.1"
       )
async def downloaded_listing_data(context: dg.AssetExecutionContext) -> None:  #, config: ListingOpConfig
    """
    The raw json files from the RapidApi containing listings data
    Returns:
        None
    """
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    context.log.info(f"Partition partition_keys: {context.partition_key_range}")
    suburb = partition_keys["suburb"]
    listing_channel = partition_keys["channel"]

    surrounding_suburbs = "false"
    exclude_under_contract = "false"
    page = 1
    page_size = 30
    api_key = os.environ.get("RAPID_API_KEY")
    # environment = os.environ.get("DAGSTER_ENVIRONMENT")

    headers = {
        'Accept': 'application/json',
        'x-rapidapi-host': 'realty-in-au.p.rapidapi.com',
        'x-rapidapi-key': api_key,
    }

    has_next_page = True

    pages_count = 0
    # num_records = 0
    data = []
    async with aiohttp.ClientSession() as session:
        # Download data until all the pages are downloaded

        while has_next_page:
            params = {
                "page": page,
                "pageSize": page_size,
                "sortType": "relevance",
                "channel": listing_channel,
                "surroundingSuburbs": surrounding_suburbs,
                "searchLocation": suburb,
                "searchLocationSubtext": "Region",
                "type": "region",
                "ex-under-contract": exclude_under_contract,
            }
            # sleep for 12 seconds (== 5 requests per minute)
            await asyncio.sleep(0.2)
            listings_data = await fetch(session, constants.RAPID_API_URL, params, headers)

            for tier in listings_data["tieredResults"]:
                if len(tier["results"]) > 0:
                    modified_data = [{**result, "suburb": suburb.lower()} for result in tier["results"]]
                    data.extend(modified_data)

            num_items = listings_data["totalResultsCount"]
            total_pages = math.ceil(num_items / page_size)
            context.log.info(f"Page: {page}/{total_pages}")
            if page < total_pages:
                page = page + 1
            else:
                has_next_page = False
                num_records = listings_data["totalResultsCount"]
                context.log.info(f"Number of records: {num_records}")
                # state = listings_data["resolvedLocalities"][0]["state"]

            pages_count += 1
    filename = constants.DOWNLOADED_REALESTATE_DATA.format(suburb=suburb, channel=listing_channel).replace(" ", "_")

    # return ReIOPayload(data=data, filepath=filename)
    # constants.ensure_directory_exists(filename)
    context.log.info(f"Filename: {filename}")
    context.log.info(f"Config: {smart_open_config.keys()}")
    with open(uri=filename, mode="wb", transport_params=smart_open_config) as f:
        f.write(json.dumps(data).encode("utf-8"))
    context.log.info(f"Completed downloading {filename}")
    # return pl.DataFrame({"s3_path": filename, "channel": listing_channel, "suburb": suburb})


class ProcessFileConfig(dg.Config):
    s3_path: str
    channel: str
    suburb: str


@op
def process_listing_data(data: list[dict], config: ProcessFileConfig):
    addresses: list[Address] = []
    agents: list[Agent] = []
    agencies: list[Agency] = []
    listings: list[Listing] = []
    rental_listings: list[RentalListing] = []
    if len(data) > 0:
        # for page in data:
        #     for tier in page["tieredResults"]:
        for listing_data in data:
            if list(listing_data.keys()).__contains__("isProject"):
                continue
            # extract features
            features_list = []
            if listing_data.keys().__contains__("propertyFeatures"):
                for feature_type in listing_data["propertyFeatures"]:
                    for feature in feature_type["features"]:
                        features_list.append(feature)
            listing_agent_ids = []
            if listing_data.keys().__contains__("listers"):
                for lister in listing_data["listers"]:
                    if len(list(lister.keys())) == 0:
                        print(f"Missing listers: {listing_data["listingId"]}")
                        continue
                    email = str(lister["email"]).strip().lower() if lister.keys().__contains__("email") else ""
                    agent = find_agent(agents, email)
                    if agent is None:
                        agency_id = str(
                            listing_data["agency"][
                                "email"]).strip().lower() if "agency" in listing_data.keys() else None
                        agent = Agent(
                            agent_id=lister["id"] if lister.keys().__contains__("id") else "",
                            full_name=lister["name"] if lister.keys().__contains__("name") else "",
                            job_title=lister["jobTitle"] if lister.keys().__contains__("jobTitle") else "",
                            email=email if lister.keys().__contains__("email") else "",
                            website=lister["website"] if lister.keys().__contains__("website") else "",
                            phone_number=lister["phoneNumber"] if lister.keys().__contains__(
                                "phoneNumber") else "",
                            mobile_number=lister["mobilePhoneNumber"] if lister.keys().__contains__(
                                "mobilePhoneNumber") else "",
                            agency_id=agency_id
                        )
                        agents.append(agent)
                    listing_agent_ids.append(agent.email)

            agency_address_id = f"{str(listing_data['agency']['address']['streetAddress']).strip()}-{str(listing_data['agency']['address']['suburb']).strip()}-{str(listing_data['agency']['address']['state']).strip()}-{str(listing_data['agency']['address']['postcode']).strip()}".replace(
                " ", "-").lower() if list(listing_data.keys()).__contains__("agency") else ""

            agency_address = find_address(addresses, agency_address_id)
            if agency_address is None and agency_address_id != "":
                agency_address = Address(
                    address_id=agency_address_id,
                    street_address=str(listing_data["agency"]["address"]["streetAddress"]).strip(),
                    suburb=str(listing_data["agency"]["address"]["suburb"]).strip(),
                    state=str(listing_data["agency"]["address"]["state"]).strip(),
                    postcode=str(listing_data["agency"]["address"]["postcode"]).strip(),
                    locality="",
                    subdivision_code="",
                    latitude=0.0,
                    longitude=0.0
                )
                addresses.append(agency_address)
            agency = None
            if listing_data.keys().__contains__("agency"):
                agency = find_agency(agencies, listing_data["agency"]["agencyId"])
                if agency is None:
                    agency = Agency(
                        agency_id=listing_data["agency"]["agencyId"],
                        name=str(listing_data["agency"]["name"]).strip(),
                        email=str(listing_data["agency"]["email"]).strip(),
                        address_id=agency_address_id,
                        website=listing_data["agency"]["website"] if list(
                            listing_data["agency"].keys()).__contains__("website") else "",
                        phone_number=listing_data["agency"]["phoneNumber"]
                    )
                    agencies.append(agency)

            listing_address_id = f"{str(listing_data['address']['streetAddress']).strip()}-{str(listing_data['address']['suburb']).strip()}-{str(listing_data['address']['state']).strip()}-{str(listing_data['address']['postcode']).strip()}".replace(
                " ", "-").lower()

            list_address = find_address(addresses, listing_address_id)

            if list_address is None:
                list_address = Address(
                    address_id=listing_address_id,
                    street_address=str(listing_data["address"]["streetAddress"]).strip(),
                    suburb=str(listing_data["address"]["suburb"]).strip(),
                    state=str(listing_data["address"]["state"]).strip(),
                    postcode=str(listing_data["address"]["postcode"]).strip(),
                    locality=str(listing_data["address"]["locality"]).strip(),
                    subdivision_code=str(listing_data["address"]["subdivisionCode"]).strip(),
                    latitude=listing_data["address"]["location"]["latitude"] if list(
                        listing_data["address"].keys()).__contains__("location") else 0.0,
                    longitude=listing_data["address"]["location"]["longitude"] if list(
                        listing_data["address"].keys()).__contains__("location") else 0.0
                )
                addresses.append(list_address)

            advertised_price = ""
            if list(listing_data.keys()).__contains__("advertising") and list(
                    listing_data["advertising"].keys()).__contains__("priceRange"):
                advertised_price = listing_data["advertising"]["priceRange"]

            if config.channel == "rent":
                listing = RentalListing(
                    listing_id=listing_data["listingId"],
                    title=listing_data["title"],
                    property_type=listing_data["propertyType"],
                    listing_type=listing_data["channel"],
                    price=listing_data["price"]["display"],
                    price_period="weekly" if "p" in str(listing_data["price"]["display"]).lower() else "monthly",
                    bond=listing_data["bond"]["value"] if "bond" in listing_data.keys() else 0,
                    bedrooms=listing_data["features"]["general"]["bedrooms"],
                    bathrooms=listing_data["features"]["general"]["bathrooms"],
                    parking_spaces=listing_data["features"]["general"]["parkingSpaces"],
                    description=listing_data["description"],
                    features=features_list,
                    status=listing_data["status"]["type"] if "status" in list(listing_data.keys()) else "",
                    date_available=listing_data["dateAvailable"]["date"],
                    classic_project=listing_data["classicProject"],
                    apply_online=listing_data["applyOnline"],
                    agency_id=agency.email if agency is not None else "",
                    agent_id=listing_agent_ids,
                    address_id=listing_address_id
                )
                rental_listings.append(listing)
            else:
                listing = Listing(
                    listing_id=listing_data["listingId"],
                    title=listing_data["title"],
                    property_type=listing_data["propertyType"] if list(listing_data.keys()).__contains__(
                        "propertyType") else "",
                    listing_type=listing_data["channel"],
                    construction_status=listing_data["constructionStatus"] if list(listing_data.keys()).__contains__(
                        "constructionStatus") else "",
                    price=listing_data["price"]["display"] if list(listing_data.keys()).__contains__("price") else "",
                    advertised_price=advertised_price,
                    bedrooms=listing_data["features"]["general"]["bedrooms"],
                    bathrooms=listing_data["features"]["general"]["bathrooms"],
                    parking_spaces=listing_data["features"]["general"]["parkingSpaces"],
                    land_size=f"{listing_data["landSize"]["value"]} {listing_data["landSize"]["unit"]}" if list(
                        listing_data.keys()).__contains__("landSize") else "",
                    description=listing_data["description"],
                    features=features_list,
                    status=listing_data["status"]["type"] if list(listing_data.keys()).__contains__(
                        "status") else "",
                    date_sold=listing_data["dateSold"]["value"] if list(listing_data.keys()).__contains__(
                        "dateSold") else "",
                    classic_project=listing_data["classicProject"],
                    agency_id=agency.email if agency is not None else "",
                    agent_id=listing_agent_ids,
                    address_id=listing_address_id
                )
                listings.append(listing)

        return addresses, agents, agencies, listings, rental_listings


@op
def normalise_listings(listing_data):
    raw_listings_pl = pl.DataFrame(listing_data)
    listings = []
    property_features = []
    listing_agents = []

    for row in raw_listings_pl.rows(named=True):
        ad_lower_price, ad_upper_price = get_ad_price(row["advertised_price"])
        price_values = re.findall(r'\$\d{1,3}(?:,\d{3})*', row["price"])
        prices = [int(str(price).replace("$", "").replace(",", "")) for price in price_values]
        price = (sum(prices) / len(prices)) if prices else 0
        listing = {
            "listing_id": row["listing_id"],
            "listing_title": row["title"],
            "property_type": row["property_type"],
            "listing_type": row["listing_type"],
            "construction_status": row["construction_status"],
            "price": price,
            "ad_lower_price": ad_lower_price,
            "ad_upper_price": ad_upper_price,
            "num_bedrooms": row["bedrooms"],
            "num_bathrooms": row["bathrooms"],
            "num_parking_spaces": row["parking_spaces"],
            "land_size": get_surface_area(row["land_size"]),
            "listing_description": row["description"],
            "listing_status": str(row["status"]).replace('"', '').strip(),
            "list_sold_date": row["date_sold"],
            "agency_id": str(row["agency_id"]).replace('"', '').strip(),
            "address_id": row["address_id"],
        }
        listings.append(listing)
        features = [{"listing_id": row["listing_id"], "feature": feature} for feature in
                    row["features"]]
        property_features.extend(features)
        for value in row["agent_id"]:
            agents = [{"listing_id": row["listing_id"], "agent_id": agent_id} for agent_id in
                      value.split(",")]
            listing_agents.extend(agents)

    return listings, listing_agents, property_features


@op
def normalise_rental_listings(listing_data):
    rental_listings = []
    property_features = []
    listing_agents = []
    raw_listings_pl = pl.DataFrame(listing_data)

    for row in raw_listings_pl.rows(named=True):
        result = re.findall(r'\d+', str(row["price"]))
        listing = {
            "listing_id": row["listing_id"],
            "listing_title": row["title"],
            "property_type": row["property_type"],
            "listing_type": row["listing_type"],
            "price": int(result[0]) if result else 0,
            "price_period": row["price_period"],
            "bond": row["bond"],
            "num_bedrooms": row["bedrooms"],
            "num_bathrooms": row["bathrooms"],
            "num_parking_spaces": row["parking_spaces"],
            "listing_description": row["description"],
            "listing_status": str(row["status"]).replace('"', '').strip(),
            "classic_project": row["classic_project"],
            "date_available": datetime.strptime(row["date_available"], "%d %b %Y"),
            "apply_online": row["apply_online"],
            "agency_id": str(row["agency_id"]).replace('"', '').strip(),
            "address_id": row["address_id"],
        }
        rental_listings.append(listing)
        features = [{"listing_id": row["listing_id"], "feature": feature} for feature in
                    row["features"]]
        property_features.extend(features)
        for value in row["agent_id"]:
            agents = [{"listing_id": row["listing_id"], "agent_id": agent_id} for agent_id in
                      value.split(",")]
            listing_agents.extend(agents)

    return rental_listings, listing_agents, property_features


@op
def get_data_objects(data):
    return [obj.__dict__ for obj in data]


@op(
    out={
        "addresses": Out(dagster_type=list[dict]),
        "agents": Out(dagster_type=list[dict]),
        "agencies": Out(dagster_type=list[dict]),
        "listings": Out(dagster_type=list[dict]),
        "rental_listings": Out(dagster_type=list[dict]),
        "listing_agents": Out(dagster_type=list[dict]),
        "property_features": Out(dagster_type=list[dict])},
)
def finalise_listing_data(data, config: ProcessFileConfig):
    if len(data) > 0:
        addresses, agents, agencies, listings, rental_listings = process_listing_data(data, config)
        # context.log.info("Finalise listing data")

        addresses_objs = get_data_objects(addresses)
        agents_objs = get_data_objects(agents)
        agencies_objs = get_data_objects(agencies)
        listings_objs = get_data_objects(listings)
        rental_listings_objs = get_data_objects(rental_listings)

        # Determine if data needs to be persisted in the database
        normalised_listings = []
        normalised_rental_listings = []

        if config.channel == "rent":
            normalised_rental_listings, listing_agents, property_features = normalise_rental_listings(
                rental_listings_objs)
        else:
            normalised_listings, listing_agents, property_features = normalise_listings(listings_objs)
        return addresses_objs, agents_objs, agencies_objs, normalised_listings, normalised_rental_listings, \
            listing_agents, property_features


@multi_asset(deps=["downloaded_listing_data"],
             outs={
                 "raw_addresses": AssetOut(metadata={"schema": "public", "table": "raw_addresses"},
                                           is_required=False,
                                           automation_condition=AutomationCondition.eager()),
                 "raw_agents": AssetOut(metadata={"schema": "public", "table": "raw_agents"},
                                        is_required=False,
                                        automation_condition=AutomationCondition.eager()),
                 "raw_agencies": AssetOut(metadata={"schema": "public", "table": "raw_agencies"},
                                          is_required=False,
                                          automation_condition=AutomationCondition.eager()),
                 "raw_listings": AssetOut(metadata={"schema": "public", "table": "raw_listings"},
                                          is_required=False,
                                          automation_condition=AutomationCondition.eager()),
                 "raw_rental_listings": AssetOut(metadata={"schema": "public", "table": "raw_rental_listings"},
                                                 is_required=False,
                                                 automation_condition=AutomationCondition.eager()),
                 "raw_listing_agents": AssetOut(metadata={"schema": "public", "table": "raw_listing_agents"},
                                                is_required=False,
                                                automation_condition=AutomationCondition.eager()),
                 "raw_listing_features": AssetOut(metadata={"schema": "public", "table": "raw_listing_features"},
                                                  is_required=False,
                                                  automation_condition=AutomationCondition.eager()),
             },
             code_version="v1.1",

             # required_resource_keys={"s3"}
             )
def normalised_listing_data(context: AssetExecutionContext,
                            s3: S3Resource,
                            config: ProcessFileConfig):
    bucket_name = dg.EnvVar("S3_BUCKET_NAME").get_value()
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=bucket_name, Key=config.s3_path)

    listing_data = json.loads(obj['Body'].read())
    addresses, agents, agencies, listings, rental_listings, listing_agents, property_features = finalise_listing_data(
        listing_data, config)

    context.log.info(f"Normalising listing data for suburb:{config.suburb} channel: {config.channel}")
    context.log.info(f"File used: {config.s3_path}")

    if len(addresses) > 0:
        yield Output(pl.DataFrame(addresses), output_name="raw_addresses")
    if len(agents) > 0:
        yield Output(pl.DataFrame(agents), output_name="raw_agents")
    if len(agencies) > 0:
        yield Output(pl.DataFrame(agencies), output_name="raw_agencies")
    if len(listings) > 0:
        yield Output(pl.DataFrame(listings), output_name="raw_listings")
    if len(rental_listings) > 0:
        yield Output(pl.DataFrame(rental_listings), output_name="raw_rental_listings")
    if len(listing_agents) > 0:
        yield Output(pl.DataFrame(listing_agents), output_name="raw_listing_agents")
    if len(property_features) > 0:
        yield Output(pl.DataFrame(property_features), output_name="raw_listing_features")
