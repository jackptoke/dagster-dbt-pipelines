import requests
from dagster_duckdb import DuckDBResource
from dagster_university.assets import constants
import dagster as dg
import pandas as pd


@dg.asset(
    group_name="raw_files",
    compute_kind="Python"
)
def raw_suburbs_file() -> dg.MaterializeResult:
    """
    The raw suburbs file as downloaded from the source.
    Returns:
        MaterializeResult (int): The number or rows found in the file.
    """
    raw_suburbs = requests.get('https://raw.githubusercontent.com/michalsn/australian-suburbs/refs/heads/master/data/suburbs.csv')

    with open(constants.AUSTRALIA_SUBURBS_FILE_PATH, "wb") as suburbs_file:
        suburbs_file.write(raw_suburbs.content)

    num_rows = len(pd.read_csv(constants.AUSTRALIA_SUBURBS_FILE_PATH))

    return dg.MaterializeResult(metadata={"Number of suburbs": dg.MetadataValue.int(num_rows)})


@dg.asset(
    deps=["raw_suburbs_file"],
    group_name="raw_data",
    compute_kind="Python"
)
def raw_suburbs(database: DuckDBResource) -> dg.MaterializeResult:
    """
    The table created from the raw suburbs file, unprocessed and uncleaned.
    Returns:
        MaterializeResult (int): The number or rows found in the file.
    """
    query = f"""
    CREATE OR REPLACE TABLE {constants.RAW_SUBURBS_TABLE} AS (
        SELECT 
            ssc_code,
            suburb,
            urban_area,
            postcode,
            state,
            state_name,
            type,
            local_goverment_area,
            statistic_area,
            elevation,
            population,
            median_income,
            sqkm,
            lat,
            lng,
            timezone
        FROM '{constants.AUSTRALIA_SUBURBS_FILE_PATH}'
    );
    """

    count_query = f"""
    SELECT COUNT(*) FROM {constants.RAW_SUBURBS_TABLE};
    """

    num_rows = 0
    with database.get_connection() as conn:
        conn.execute(query)
        num_rows = conn.execute(count_query).fetchone()[0]

    return dg.MaterializeResult(metadata={"Number of suburbs": dg.MetadataValue.int(num_rows)})

