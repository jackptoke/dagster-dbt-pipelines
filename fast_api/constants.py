import os

LISTINGS_QUERY = """
    SELECT
    listing_id,
    title,
    price,
    property_type,
    bedrooms,
    bathrooms,
    parking,
    land,
    sold_date,
    channel,
    address,
    suburb,
    state,
    postcode,
    latitude,
    longitude
FROM realestate_dev.main.fct_listing
WHERE LOWER(channel) = LOWER('sold') AND LOWER(state) = LOWER(?) AND LOWER(suburb) = LOWER(?) AND date_part('year', sold_date::DATE) = ?
    """

SUBURBS_QUERY = """
SELECT DISTINCT suburb
FROM realestate_dev.main.fct_listing
WHERE state = LOWER(?) 
ORDER BY suburb
"""

STATES_QUERY = """
SELECT DISTINCT state
FROM realestate_dev.main.fct_listing
ORDER BY state
"""

DUCKDB_FILE = f'{os.getenv("MOTHER_DUCK_URL")}?motherduck_token={os.getenv("MOTHER_DUCK_TOKEN")}'
