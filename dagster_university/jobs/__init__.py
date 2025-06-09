import dagster as dg

from ..assets.dbt import dbt_analytics
from ..assets.listings import downloaded_listing_data, process_downloaded_listing_data, cleansed_listings_data, \
    cleansed_rental_listings_data

download_listing_data_job = dg.define_asset_job(
    name="download_listing_data_job",
    selection=[downloaded_listing_data],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 5})
)

process_downloaded_listing_data_job = dg.define_asset_job(
    name="process_download_listing_data_job",
    selection=[process_downloaded_listing_data],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1}),
)

raw_listing_data_job = dg.define_asset_job(
    name="raw_listing_data_job",
    selection=[cleansed_listings_data],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1})
)

raw_rental_listing_data_job = dg.define_asset_job(
    name="raw_rental_listing_data_job",
    selection=[cleansed_rental_listings_data],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1})
)

rebuild_dbt_assets_job = dg.define_asset_job(
    name="rebuild_dbt_assets_job",
    selection=[dbt_analytics],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1})
)

# trips_by_week = dg.AssetSelection.assets("trips_by_week")
# adhoc_request = dg.AssetSelection.assets("adhoc_request")
#
# trip_update_job = dg.define_asset_job(
#     name="trip_update_job",
#     partitions_def=monthly_partition,
#     selection=dg.AssetSelection.all() - trips_by_week - adhoc_request,
# )
#
# weekly_update_job = dg.define_asset_job(
#     name="weekly_update_job", partitions_def=weekly_partition, selection=trips_by_week
# )
#
# adhoc_request_job = dg.define_asset_job(name="adhoc_request_job", selection=adhoc_request)
