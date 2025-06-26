import json
import re
from datetime import datetime

import dagster as dg
from dagster import SensorEvaluationContext, op, RunsFilter, DagsterRunStatus, SkipReason, AssetKey, AssetSelection
from dagster_aws.s3 import S3Resource

from ..jobs import normalised_listing_data_job

INTERVAL_TIME = 30


# Check if a file is a json file
@op
def is_json_file(filename):
    return bool(re.match(r'^.+\.(json)$', filename, re.IGNORECASE))


@dg.sensor(
    job=normalised_listing_data_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=120,
    tags={"dagster/priority": "5"},
)
def normalised_listing_data_sensor(context: SensorEvaluationContext, s3: S3Resource):
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name="normalised_listing_data_job",
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

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}

    bucket_name = dg.EnvVar("S3_BUCKET_NAME").get_value()
    folder_prefix = "data/downloaded/"
    # context.log.info(f"Bucket: {bucket_name}")
    s3_client = s3.get_client()
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

    for file in response['Contents']:
        context.log.info(f"S3 file key: {file['Key']}")
        s3_file_path = file['Key']

        # Ignore directories
        if is_json_file(s3_file_path):
            current_state[s3_file_path] = file["LastModified"]

            if s3_file_path not in previous_state or previous_state[s3_file_path] != file["LastModified"]:
                file_parts = s3_file_path.split('/')
                filename = file_parts[-1]
                partitions = filename.split("_")
                channel = partitions[-2]
                suburb = " ".join(partitions[:-2])

                config = {
                    "ops": {
                        "normalised_listing_data": {
                            "config": {"channel": channel, "suburb": suburb,
                                       "s3_path": f"{s3_file_path}"}
                        }
                    }
                }
                # context.log.info(f"S3 file path: {s3_file_path} Config: {json.dumps(config)}")
                yield dg.RunRequest(
                    run_key=f"normalised_listing_data_{suburb}_{channel}",
                    run_config=config,
                )
            else:
                yield dg.SkipReason(f"{s3_file_path} file has already been processed")
        else:
            yield dg.SkipReason(f"{s3_file_path} is not a JSON file")


#
# @dg.multi_asset_sensor(
#     monitored_assets=[AssetKey("raw_listings"), AssetKey("raw_rental_listings"), AssetKey("raw_addresses"),
#                       AssetKey("raw_agencies"), AssetKey("raw_agents"), AssetKey("raw_listing_agents"),
#                       AssetKey("raw_listing_features"), AssetKey("raw_suburbs")],
#     request_assets=AssetSelection.groups("DBT_STAGING_DATA") | AssetSelection.groups("DBT_DIM_DATA") | AssetSelection.groups("DBT_FACT_DATA"),
#     default_status=dg.DefaultSensorStatus.RUNNING,
#     minimum_interval_seconds=INTERVAL_TIME)
# def materialise_dbt_assets(context: SensorEvaluationContext):
#     run_records = context.instance.get_run_records(
#         RunsFilter(
#             job_name="rebuild_dbt_assets_job",
#             statuses=[
#                 DagsterRunStatus.QUEUED,
#                 DagsterRunStatus.NOT_STARTED,
#                 DagsterRunStatus.STARTING,
#                 DagsterRunStatus.STARTED
#             ]
#         )
#     )
#     previous_state = json.loads(context.cursor) if context.cursor else {}
#     current_state = {}
#
#     if len(run_records) > 0:
#         yield SkipReason("Skipping this run because another run of the same job is already running")
#     else:
#         now = datetime.now()
#         date_str = now.strftime("%Y-%m-%d-%H")
#         state_key = f"materialise_dbt_assets-{date_str}-{(now.minute / 5)}"
#         current_state[state_key] = now.strftime("%Y-%m-%d-%H-%M")
#         if state_key not in previous_state or previous_state[state_key] != current_state[state_key]:
#             yield dg.RunRequest()
#         else:
#             yield SkipReason(f"{state_key} has already been processed")
#
