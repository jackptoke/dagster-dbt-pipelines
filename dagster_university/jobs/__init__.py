import dagster as dg

from ..assets.dbt import dbt_analytics
from ..assets.listings import downloaded_listing_data, normalised_listing_data

download_listing_data_job = dg.define_asset_job(
    name="download_listing_data_job",
    selection=[downloaded_listing_data],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 5})
)

normalised_listing_data_job = dg.define_asset_job(
    name="normalised_listing_data_job",
    selection=[normalised_listing_data],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1}),
)

rebuild_dbt_assets_job = dg.define_asset_job(
    name="rebuild_dbt_assets_job",
    selection=[dbt_analytics],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1})
)
