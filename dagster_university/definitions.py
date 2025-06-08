import dagster as dg

from dagster_university.assets import suburbs, dbt, listings
from dagster_university.io.re_io_manager import ReIOManager
from dagster_university.jobs import download_listing_data_job, process_downloaded_listing_data_job, \
    raw_listing_data_job, \
    raw_rental_listing_data_job, rebuild_dbt_assets_job
from dagster_university.resources import dbt_resource, io_manager, database_resource
from dagster_university.schedules import download_listing_schedule
from dagster_university.sensors import downloaded_listing_data_sensor, raw_listings_sensor, raw_rental_listings_sensor, \
    staging_listings_sensor, staging_rental_listings_sensor, raw_suburbs_sensor

suburbs_assets = dg.load_assets_from_modules([suburbs])
listings_assets = dg.load_assets_from_modules([listings])
dbt_analytics_assets = dg.load_assets_from_modules(modules=[dbt])


defs = dg.Definitions(
    assets=[*suburbs_assets, *listings_assets, *dbt_analytics_assets],
    resources={
        "duckdb": database_resource,
        "dbt": dbt_resource,
        "io_manager": io_manager,
        "re_io_manager": ReIOManager()
    },
    jobs=[download_listing_data_job, process_downloaded_listing_data_job, raw_listing_data_job,
          raw_rental_listing_data_job, rebuild_dbt_assets_job],
    schedules=[download_listing_schedule],
    sensors=[downloaded_listing_data_sensor, raw_listings_sensor, raw_rental_listings_sensor, staging_listings_sensor, \
             staging_rental_listings_sensor, raw_suburbs_sensor],
    executor=dg.multiprocess_executor.configured({"max_concurrent": 2})
)
