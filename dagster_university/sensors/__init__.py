# import json
import os
import json
import dagster as dg
from dagster import SensorEvaluationContext

from ..jobs import process_downloaded_listing_data_job


@dg.sensor(
    job=process_downloaded_listing_data_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=10)
def downloaded_listing_data_sensor(context: SensorEvaluationContext):
    PATH_TO_DOWNLOADED_FILES = os.path.join(os.path.dirname(__file__), "../../", "data/downloaded")

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    # runs_to_request = []

    for filename in os.listdir(PATH_TO_DOWNLOADED_FILES):
        file_path = os.path.join(PATH_TO_DOWNLOADED_FILES, filename)
        if filename.endswith(".json") and os.path.isfile(file_path):
            last_modified = os.path.getmtime(file_path)

            current_state[filename] = last_modified

            # if the file is new or has been modified since the last run, add it to the request queue
            if filename not in previous_state or previous_state[filename] != last_modified:
                # with open(file_path) as f:
                #     request_config = json.load(f)
                yield dg.RunRequest(
                    run_key=f"downloaded_listing_data_{filename}_{last_modified}",
                    run_config={
                        "ops": {
                            "process_downloaded_listing_data": {
                                "config": {"filename": filename, "filepath": file_path}
                            }
                        }
                    },
                )
            else:
                yield dg.SkipReason("No new files found")

    # return dg.SensorResult(run_requests=runs_to_request, cursor=json.dumps(current_state))


@dg.asset_sensor(asset_key=dg.AssetKey("raw_listings"), job_name="raw_listing_data_job",
                 default_status=dg.DefaultSensorStatus.RUNNING)
def raw_listings_sensor(context: SensorEvaluationContext):
    yield dg.RunRequest()


@dg.asset_sensor(asset_key=dg.AssetKey("raw_rental_listings"), job_name="raw_rental_listing_data_job",
                 default_status=dg.DefaultSensorStatus.RUNNING)
def raw_rental_listings_sensor(context: SensorEvaluationContext):
    yield dg.RunRequest()
