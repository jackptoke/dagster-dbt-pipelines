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
WHERE LOWER(channel) = LOWER(:channel) AND LOWER(state) = LOWER(:state) AND LOWER(suburb) = LOWER(:suburb) AND date_part('year', sold_date::DATE) = :year
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
DUCKDB_CONN = f"duckdb:///{DUCKDB_FILE}"
DUCKDB_CONFIG = {'motherduck_token': os.getenv("MOTHER_DUCK_TOKEN")}

