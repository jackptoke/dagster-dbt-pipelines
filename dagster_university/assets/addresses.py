import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_university.assets import constants


# @dg.asset(
#     group_name="raw_data",
#     deps=["realestate_listings_files"],
#     description="Raw addresses data generated from addresses CSV file",
#     compute_kind="Python"
# )
# def raw_addresses(database: DuckDBResource) -> dg.MaterializeResult:
#     query = f"""
#     CREATE OR REPLACE TABLE realestate.raw_addresses AS (
#         SELECT
#             address_id,
#             street_address,
#             suburb,
#             state,
#             postcode,
#             locality,
#             subdivision_code,
#             latitude,
#             longitude
#         FROM '{constants.ADDRESSES_CSV_FILE}'
#     );
#     """
#
#     count_query = "SELECT COUNT(*) FROM realestate.raw_addresses;"
#
#     num_rows = 0
#
#     with database.get_connection() as conn:
#         conn.execute(query)
#         num_rows = conn.execute(count_query).fetchone()[0]
#
#     return dg.MaterializeResult(metadata={"Number of addresses": dg.MetadataValue.int(num_rows)})
