import dagster as dg

from dagster_university.io.re_io_manager import ReIOManager
from dagster_university.resources import io_manager, dbt_resource, database_resource
from dagster_university.assets import suburbs, dbt, listings
from dagster_university.schedules import suburb_channel_weekly_schedule, raw_suburbs_file_schedule
from dagster_university.jobs import download_listing_data_job, process_downloaded_listing_data_job, \
    raw_listing_data_job, \
    raw_rental_listing_data_job, rebuild_dbt_assets_job, raw_suburbs_file_job
from dagster_university.sensors import downloaded_listing_data_sensor, raw_listings_sensor, raw_rental_listings_sensor, \
    staging_listings_sensor, staging_rental_listings_sensor

suburbs_assets = dg.load_assets_from_modules([suburbs])
listings_assets = dg.load_assets_from_modules([listings])
dbt_analytics_assets = dg.load_assets_from_modules(modules=[dbt])


defs = dg.Definitions(
    assets=[*suburbs_assets, *listings_assets, *dbt_analytics_assets],
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
        "io_manager": io_manager,
        "re_io_manager": ReIOManager()
    },
    jobs=[download_listing_data_job, process_downloaded_listing_data_job, raw_listing_data_job,
          raw_rental_listing_data_job, rebuild_dbt_assets_job, raw_suburbs_file_job],
    schedules=[suburb_channel_weekly_schedule, raw_suburbs_file_schedule],
    sensors=[downloaded_listing_data_sensor, raw_listings_sensor, raw_rental_listings_sensor, staging_listings_sensor, \
             staging_rental_listings_sensor],
    executor=dg.multiprocess_executor.configured({"max_concurrent": 2})
)
