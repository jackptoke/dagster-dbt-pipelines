import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_university.assets import constants


# @dg.asset(
#     group_name="raw_data",
#     deps=["realestate_listings_files"],
#     description="Raw data assets for agencies",
#     compute_kind="Python"
# )
# def raw_agencies(database: DuckDBResource) -> dg.MaterializeResult:
#     query = f"""
#     CREATE OR REPLACE TABLE realestate.raw_agencies AS (
#     SELECT
#         agency_id,
#         name,
#         email,
#         address_id,
#         website,
#         phone_number
#     FROM '{constants.AGENCIES_CSV_FILE}'
#     );
#     """
#
#     count_query = "SELECT COUNT(*) FROM realestate.raw_agencies"
#     num_rows = 0
#     with database.get_connection() as conn:
#         conn.execute(query)
#         num_rows = conn.execute(count_query).fetchone()[0]
#
#     return dg.MaterializeResult(metadata={"Number of agencies": dg.MetadataValue.int(num_rows)})
