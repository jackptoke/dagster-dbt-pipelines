from dagster import schedule, RunRequest, MultiPartitionKey, RunsFilter, \
    DagsterRunStatus, SkipReason

from ..jobs import download_listing_data_job
from ..partitions import SUBURBS, CHANNELS


# suburb_channel_weekly_schedule = build_schedule_from_partitioned_job(
#     download_listing_data_job,
#     name="suburb_channel_weekly_schedule",
#     cron_schedule="0 0 * * 1",
#     execution_timezone="Australia/Melbourne"
# )

def incomplete_partition(partition):
    return not partition["status"]


# Run every monday at 12:00
@schedule(cron_schedule="0 0 * * *", job=download_listing_data_job)
def download_listing_schedule(context):
    job_partitions = [{"suburb": suburb, "channel": channel} for suburb in SUBURBS for channel in
                      CHANNELS]

    while len(job_partitions) > 0:
        run_records = context.instance.get_run_records(
            RunsFilter(
                job_name="download_listing_data_job",
                statuses=[
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED
                ]
            )
        )

        if len(run_records) > 0:
            yield SkipReason("Skipping this run because another run of the same job is already running")
        else:
            job = job_partitions.pop(0)
            yield RunRequest(
                run_key=f"{job["suburb"]}-{job["channel"]}",
                tags={"suburb": job["suburb"], "channel": job["channel"]},
                partition_key=MultiPartitionKey({"suburb": job["suburb"], "channel": job["channel"]}),
            )


# raw_suburbs_file_schedule = ScheduleDefinition(job=raw_suburbs_file_job,
#                                                cron_schedule="0 0 1 * *",)


    # for suburb in SUBURBS:
    #     for channel in CHANNELS:
    #         yield RunRequest(
    #             run_key=f"{suburb}-{channel}",
    #             tags={"suburb": suburb, "channel": channel},
    #             partition_key=MultiPartitionKey({"suburb": suburb, "channel": channel}),
    #             # run_config={
    #             #     "ops": {
    #             #         "downloaded_listing_data": {
    #             #             "config": {"suburb": suburb, "channel": channel},
    #             #         }
    #             #     }
    #             # }
    #         )


# download_listing_data_schedule = build_schedule_from_partitioned_job(
#     download_listing_data_job,
# )
