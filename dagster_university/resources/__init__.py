import logging
import os

import boto3
import dagster as dg
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster_university.project import dbt_project

logger = logging.getLogger(__name__)

db_conn = f"{dg.EnvVar("MOTHER_DUCK_URL").get_value()}?motherduck_token={dg.EnvVar("MOTHER_DUCK_TOKEN").get_value()}" #if dg.EnvVar("DAGSTER_ENVIRONMENT").get_value() == "prod" else dg.EnvVar("DUCKDB_DATABASE").get_value()
logger.info(f"DB Connection: Connecting to {db_conn}")
database_resource = DuckDBResource(database=db_conn)

io_manager = DuckDBPandasIOManager(database=db_conn, schema="public")

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

DUCKDB_LOCAL_CONFIG = """
set s3_access_key_id='minio';
set s3_secret_access_key='minio123';
set s3_endpoint='minio:9000';
set s3_use_ssl='false';
set s3_url_style='path';
"""

# mother_duck_resource = MotherduckIOManager(DuckDB(url=dg.EnvVar("MOTHER_DUCK_URL")))

if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}
