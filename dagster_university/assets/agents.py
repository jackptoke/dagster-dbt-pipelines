import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_university.assets import constants


# @dg.asset(
#     group_name="raw_data",
#     deps=["realestate_listings_files"],
#     description="Raw data asset for agents",
#     compute_kind="Python"
# )
# def raw_agents(database: DuckDBResource) -> dg.MaterializeResult:
#     query = f"""
#     CREATE OR REPLACE TABLE realestate.raw_agents AS (
#         SELECT
#             agent_id,
#             full_name,
#             job_title,
#             email,
#             website,
#             phone_number,
#             mobile_number,
#             agency_id
#         FROM '{constants.AGENTS_CSV_FILE}'
#     );
#     """
#
#     count_query = "SELECT COUNT(*) FROM realestate.raw_agents"
#
#     num_rows = 0
#     with database.get_connection() as conn:
#         conn.execute(query)
#         num_rows = conn.execute(count_query).fetchone()[0]
#
#     return dg.MaterializeResult(metadata={"Number of agents": dg.MetadataValue.int(num_rows)})
