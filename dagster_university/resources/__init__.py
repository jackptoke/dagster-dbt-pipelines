import os

import boto3
import dagster as dg

from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster_university.io.re_io_manager import ReIOManager
from dagster_university.project import dbt_project

database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE"),
)

io_manager = DuckDBPandasIOManager(database=dg.EnvVar("DUCKDB_DATABASE"), schema="public")

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}
