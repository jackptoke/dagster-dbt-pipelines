import dagster as dg

from dagster_university.resources import io_manager, dbt_resource, database_resource
from dagster_university.assets import suburbs, dbt, listings

suburbs_assets = dg.load_assets_from_modules([suburbs])
listings_assets = dg.load_assets_from_modules([listings])
dbt_analytics_assets = dg.load_assets_from_modules(modules=[dbt])

defs = dg.Definitions(
    assets=[*suburbs_assets, *listings_assets, *dbt_analytics_assets],
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
        "io_manager": io_manager
    }
)
