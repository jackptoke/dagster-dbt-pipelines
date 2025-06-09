import json
import re

import dagster as dg
from dagster import SensorEvaluationContext, RunsFilter, DagsterRunStatus, SkipReason, op
from dagster_aws.s3 import S3Resource

from ..jobs import process_downloaded_listing_data_job

INTERVAL_TIME = 10


# Check if a file is a json file
@op
def is_json_file(filename):
    return bool(re.match(r'^.+\.(json)$', filename, re.IGNORECASE))


@dg.sensor(
    job=process_downloaded_listing_data_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=180,
    tags={"dagster/priority": "5"},
)
def downloaded_listing_data_sensor(context: SensorEvaluationContext, s3: S3Resource):
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name="process_download_listing_data_job",
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
            ],
        )
    )

    if len(run_records) > 0:
        return dg.SkipReason("Skipping because there is already another run of the same job is already running")

    bucket_name = "realestate"
    folder = "data/downloaded/"

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    # runs_to_request = []

    s3_client = s3.get_client()

    file_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)

    for file in file_list['Contents']:
        context.log.info(f"S3 file key: {file['Key']}")
        s3_file_path = file['Key']

        # Ignore directories
        if is_json_file(s3_file_path):
            current_state[s3_file_path] = file["LastModified"]

            if s3_file_path not in previous_state or previous_state[s3_file_path] != file["LastModified"]:
                filename = s3_file_path.split('/')[-1]
                file_path = s3_file_path.split('/')[:-1]
                config = {
                    "ops": {
                        "process_downloaded_listing_data": {
                            "config": {"filename": filename, "filepath": f"{file_path}",
                                       "s3_path": f"{s3_file_path}"}
                        }
                    }
                }
                context.log.info(f"S3 file path: {s3_file_path} Config: {json.dumps(config)}")
                yield dg.RunRequest(
                    run_key=f"downloaded_listing_data_{filename}_{file["LastModified"]}",
                    run_config=config,
                )
            else:
                yield dg.SkipReason(f"{s3_file_path} file has already been processed")
        else:
            yield dg.SkipReason(f"{s3_file_path} is not a JSON file")
    # for filename in os.listdir(path_to_downloaded_files):
    #     file_path = os.path.join(path_to_downloaded_files, filename)
    #     if filename.endswith(".json") and os.path.isfile(file_path):
    #         last_modified = os.path.getmtime(file_path)
    #
    #         current_state[filename] = last_modified
    #
    #         # if the file is new or has been modified since the last run, add it to the request queue
    #         if filename not in previous_state or previous_state[filename] != last_modified:
    #             # with open(file_path) as f:
    #             #     request_config = json.load(f)
    #             yield dg.RunRequest(
    #                 run_key=f"downloaded_listing_data_{filename}_{last_modified}",
    #                 run_config={
    #                     "ops": {
    #                         "process_downloaded_listing_data": {
    #                             "config": {"filename": filename, "filepath": file_path, "s3_path": s3_file_path}
    #                         }
    #                     }
    #                 },
    #             )
    #         else:
    #             yield dg.SkipReason("No new files found")

    # return dg.SensorResult(run_requests=runs_to_request, cursor=json.dumps(current_state))


@dg.asset_sensor(asset_key=dg.AssetKey("raw_listings"), job_name="raw_listing_data_job",
                 default_status=dg.DefaultSensorStatus.RUNNING,
                 minimum_interval_seconds=INTERVAL_TIME)
def raw_listings_sensor(context: SensorEvaluationContext):
    yield dg.RunRequest()


@dg.asset_sensor(asset_key=dg.AssetKey("raw_rental_listings"), job_name="raw_rental_listing_data_job",
                 default_status=dg.DefaultSensorStatus.RUNNING,
                 minimum_interval_seconds=INTERVAL_TIME)
def raw_rental_listings_sensor(context: SensorEvaluationContext):
    yield dg.RunRequest()


@dg.asset_sensor(asset_key=dg.AssetKey("staging_listings"), job_name="rebuild_dbt_assets_job",
                 default_status=dg.DefaultSensorStatus.RUNNING,
                 minimum_interval_seconds=INTERVAL_TIME)
def staging_listings_sensor(context: SensorEvaluationContext):
    run_records = context.instance.get_run_records(
        RunsFilter(
            job_name="rebuild_dbt_assets_job",
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
        yield dg.RunRequest()


@dg.asset_sensor(asset_key=dg.AssetKey("staging_rental_listings"), job_name="rebuild_dbt_assets_job",
                 default_status=dg.DefaultSensorStatus.RUNNING,
                 minimum_interval_seconds=INTERVAL_TIME)
def staging_rental_listings_sensor(context: SensorEvaluationContext):
    run_records = context.instance.get_run_records(
        RunsFilter(
            job_name="rebuild_dbt_assets_job",
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
        yield dg.RunRequest()


@dg.asset_sensor(asset_key=dg.AssetKey("raw_suburbs"), job_name="rebuild_dbt_assets_job",
                 default_status=dg.DefaultSensorStatus.RUNNING,
                 minimum_interval_seconds=INTERVAL_TIME)
def raw_suburbs_sensor(context: SensorEvaluationContext):
    run_records = context.instance.get_run_records(
        RunsFilter(
            job_name="rebuild_dbt_assets_job",
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
        yield dg.RunRequest()
