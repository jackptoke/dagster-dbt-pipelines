from dagster import schedule, RunRequest, MultiPartitionKey
from dagster_dbt import build_schedule_from_dbt_selection, dbt_assets

from ..jobs import download_listing_data_job, rebuild_dbt_assets_job
from ..partitions import SUBURBS, CHANNELS
from ..assets.dbt import dbt_analytics


# suburb_channel_weekly_schedule = build_schedule_from_partitioned_job(
#     download_listing_data_job,
#     name="suburb_channel_weekly_schedule",
#     cron_schedule="0 0 * * 1",
#     execution_timezone="Australia/Melbourne"
# )

def incomplete_partition(partition):
    return not partition["status"]


# Run every monday at 12:00
@schedule(cron_schedule="0 0 * * 1",
          job=download_listing_data_job,
          execution_timezone="Australia/Melbourne")
def download_listing_schedule(context):
    job_partitions = [{"suburb": suburb, "channel": channel} for suburb in SUBURBS for channel in
                      CHANNELS]

    for job in job_partitions:
        yield RunRequest(
            run_key=f"{job["suburb"]}-{job["channel"]}",
            tags={"suburb": job["suburb"], "channel": job["channel"]},
            partition_key=MultiPartitionKey({"suburb": job["suburb"], "channel": job["channel"]}),
        )


materialise_dbt_assets_schedule = build_schedule_from_dbt_selection(
    [dbt_analytics],
    job_name="materialise_dbt_model",
    cron_schedule="0 2 * * *",
    # dbt_select="fqn:*",
    execution_timezone="Australia/Melbourne"
)
