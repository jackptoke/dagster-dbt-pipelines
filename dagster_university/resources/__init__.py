import logging
import os

import boto3
import dagster as dg
from botocore.client import Config
from dagster_aws.s3 import S3Resource
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from dagster_duckdb_polars import DuckDBPolarsIOManager

from dagster_university.project import dbt_project

logger = logging.getLogger(__name__)

db_conn = f"{dg.EnvVar("MOTHER_DUCK_URL").get_value()}?motherduck_token={dg.EnvVar("MOTHER_DUCK_TOKEN").get_value()}"  #if dg.EnvVar("DAGSTER_ENVIRONMENT").get_value() == "prod" else dg.EnvVar("DUCKDB_DATABASE").get_value()
logger.info(f"DB Connection: Connecting to {db_conn}")
database_resource = DuckDBResource(database=db_conn)

# IO Manager that materialise a dataframe into a table on DuckDB
io_manager = DuckDBPolarsIOManager(database=db_conn, schema="public")

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

if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.session.Session()
    smart_open_config = {"client": session.client(
        service_name="s3",
        endpoint_url="http://172.16.1.11:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        config=Config(signature_version="s3v4"),
    )}
else:
    smart_open_config = {}

s3_resource = S3Resource(
    endpoint_url=dg.EnvVar("MINIO_URL").get_value(),
    aws_access_key_id=dg.EnvVar("MINIO_KEY").get_value(),
    aws_secret_access_key=dg.EnvVar("MINIO_SECRET").get_value(),
)
# Initialise a session using Spaces
