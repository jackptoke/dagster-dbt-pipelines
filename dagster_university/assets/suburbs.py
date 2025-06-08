import dagster as dg
import pandas as pd
import requests
from dagster import AutomationCondition

from dagster_university.assets import constants


@dg.asset(
    group_name="raw_files",
    compute_kind="Python",
    automation_condition=AutomationCondition.on_cron("@monthly")
)
def raw_suburbs_file() -> dg.MaterializeResult:
    """
    The raw suburbs file as downloaded from the source.
    """
    raw_suburbs = requests.get(
        'https://raw.githubusercontent.com/michalsn/australian-suburbs/refs/heads/master/data/suburbs.csv')

    with open(constants.AUSTRALIA_SUBURBS_FILE_PATH, "wb") as suburbs_file:
        suburbs_file.write(raw_suburbs.content)

    num_rows = len(pd.read_csv(constants.AUSTRALIA_SUBURBS_FILE_PATH))

    return dg.MaterializeResult(metadata={"Number of suburbs": dg.MetadataValue.int(num_rows)})


@dg.asset(
    deps=["raw_suburbs_file"],
    group_name="raw_data",
    compute_kind="duckdb",
    automation_condition=AutomationCondition.eager()
)
def raw_suburbs() -> pd.DataFrame:
    """
    The table created from the raw suburbs file, unprocessed and uncleaned.
    """

    suburbs_df = pd.read_csv(constants.AUSTRALIA_SUBURBS_FILE_PATH)

    return suburbs_df
