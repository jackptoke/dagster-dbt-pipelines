import os

S3_BUCKET_PREFIX = os.getenv("S3_BUCKET_PREFIX", "s3://dagster-university/")


def ensure_directory_exists(file_path):
    """
    Ensures the directory for the given file path exists.
    Returns the directory path.

    Args:
        file_path (str): Path to the file

    Returns:
        Path: Path object representing the directory
    """
    from pathlib import Path
    # Convert to Path object if string is provided
    path = Path(file_path)

    # Get the directory path
    directory = path.parent

    # Create directory if it doesn't exist
    directory.mkdir(parents=True, exist_ok=True)

    return directory


def get_path_for_env(path: str) -> str:
    """A utility method for Dagster University. Generates a path based on the environment.

    Args:
        path (str): The local path to the file.

    Returns:
        result_path (str): The path to the file, based on the environment.
    """
    if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
        return S3_BUCKET_PREFIX + path
    else:
        return path


TAXI_ZONES_FILE_PATH = get_path_for_env("data/raw/taxi_zones.csv")
TAXI_TRIPS_TEMPLATE_FILE_PATH = get_path_for_env("data/raw/taxi_trips_{}.parquet")

TAXI_ZONES_FILE_PATH = get_path_for_env(os.path.join("data", "raw", "taxi_zones.csv"))
TAXI_TRIPS_TEMPLATE_FILE_PATH = get_path_for_env(
    os.path.join("data", "raw", "taxi_trips_{}.parquet")
)

TRIPS_BY_AIRPORT_FILE_PATH = get_path_for_env(
    os.path.join("data", "outputs", "trips_by_airport.csv")
)
TRIPS_BY_WEEK_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "trips_by_week.csv"))
MANHATTAN_STATS_FILE_PATH = get_path_for_env(
    os.path.join("data", "staging", "manhattan_stats.geojson")
)
MANHATTAN_MAP_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "manhattan_map.png"))

REQUEST_DESTINATION_TEMPLATE_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "{}.png"))

DATE_FORMAT = "%Y-%m-%d"

START_DATE = "2023-01-01"
END_DATE = "2023-04-01"

AIRPORT_TRIPS_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "airport_trips.png"))

## Lewagon
AUSTRALIA_SUBURBS_FILE_PATH = get_path_for_env("data/raw/australia_suburbs.csv")
REALESTATE_LISTING_RAW_FILE_PATH = get_path_for_env(os.path.join("data", "raw", "{channel}", "{state}", "{suburb}", "{date_str}-{suburb}.json"))
REALESTATE_LISTING_STAGING_FILE_PATH = get_path_for_env("data/staging/{channel}/{state}/{suburb}/{date-str}-{suburb}.json")

DOWNLOADED_REALESTATE_DATA = get_path_for_env(os.path.join("data", "raw", "downloaded.json"))

RAPID_API_URL=f"https://realty-in-au.p.rapidapi.com/properties/list"

SQLITE3_FILE = get_path_for_env("data/staging/database.sqlite")
ADDRESSES_CSV_FILE = get_path_for_env("data/staging/csv/addresses.csv")
AGENTS_CSV_FILE = get_path_for_env("data/staging/csv/agents.csv")
AGENCIES_CSV_FILE = get_path_for_env("data/staging/csv/agencies.csv")
LISTINGS_CSV_FILE = get_path_for_env("data/staging/csv/listings.csv")

NORMALISED_LISTINGS_FILE = get_path_for_env("data/normalised/listings.csv")
NORMALISED_LISTING_AGENTS_FILE = get_path_for_env("data/normalised/listing_agents.csv")
NORMALISED_LISTING_FEATURES_FILE = get_path_for_env("data/normalised/listing_features.csv")

RAW_LISTINGS_TABLE = "public.raw_listings"
RAW_RENTAL_LISTINGS_TABLE = "public.raw_rental_listings"
RAW_AGENTS_TABLE = "public.raw_agents"
RAW_AGENCIES_TABLE = "public.raw_agencies"
RAW_ADDRESSES_TABLE = "public.raw_addresses"
RAW_SUBURBS_TABLE = "public.raw_suburbs"
STAGING_LISTINGS_TABLE = "public.stg_listings"
STAGING_AGENTS_TABLE = "public.stg_agents"
STAGING_AGENCIES_TABLE = "public.stg_agencies"
STAGING_ADDRESSES_TABLE = "public.stg_addresses"
STAGING_LISTING_FEATURES_TABLE = "public.stg_listing_features"
STAGING_LISTING_AGENTS_TABLE = "public.stg_listing_agents"
NORMALISED_LISTINGS_TABLE = "public.normalised_listings"
NORMALISED_LISTING_AGENTS_TABLE = "public.normalised_listing_agents"
NORMALISED_LISTING_FEATURES_TABLE = "public.normalised_listing_features"
