import dagster as dg

from dagster_university.assets import suburbs, dbt, listings
from dagster_university.io.re_io_manager import ReIOManager
from dagster_university.jobs import download_listing_data_job, \
    normalised_listing_data_job, rebuild_dbt_assets_job
from dagster_university.resources import dbt_resource, io_manager, database_resource, s3_resource
from dagster_university.schedules import download_listing_schedule, materialise_dbt_assets_schedule
from dagster_university.sensors import normalised_listing_data_sensor
suburbs_assets = dg.load_assets_from_modules([suburbs])
listings_assets = dg.load_assets_from_modules([listings])
dbt_analytics_assets = dg.load_assets_from_modules(modules=[dbt])


defs = dg.Definitions(
    assets=[*suburbs_assets, *listings_assets, *dbt_analytics_assets],
    sensors=[normalised_listing_data_sensor,],
    jobs=[download_listing_data_job, normalised_listing_data_job, rebuild_dbt_assets_job],
    schedules=[download_listing_schedule, materialise_dbt_assets_schedule],
    resources={
            "duckdb": database_resource,
            "dbt": dbt_resource,
            "io_manager": io_manager,
            "re_io_manager": ReIOManager(),
            "s3": s3_resource,
        },
    executor=dg.multiprocess_executor.configured({"max_concurrent": 2})
)
