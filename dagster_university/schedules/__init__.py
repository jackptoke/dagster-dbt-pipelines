from dagster import build_schedule_from_partitioned_job, schedule, RunRequest, MultiPartitionKey

from ..jobs import download_listing_data_job
from ..partitions import SUBURBS, CHANNELS


# suburb_channel_weekly_schedule = build_schedule_from_partitioned_job(
#     download_listing_data_job,
#     name="suburb_channel_weekly_schedule",
#     cron_schedule="0 0 * * 1",
#     execution_timezone="Australia/Melbourne"
# )

# Run every monday at 12:00
@schedule(cron_schedule="0 0 * * 1", job=download_listing_data_job)
def suburb_channel_weekly_schedule():
    for suburb in SUBURBS:
        for channel in CHANNELS:
            yield RunRequest(
                run_key=f"{suburb}-{channel}",
                tags={"suburb": suburb, "channel": channel},
                partition_key=MultiPartitionKey({"suburb": suburb, "channel": channel}),
                # run_config={
                #     "ops": {
                #         "downloaded_listing_data": {
                #             "config": {"suburb": suburb, "channel": channel},
                #         }
                #     }
                # }
            )
