import dagster as dg
from dagster import StaticPartitionsDefinition, DynamicPartitionsDefinition, StaticPartitionMapping, AssetIn, \
    PartitionMapping
from ..assets import constants
from typing import Mapping


def get_partition_mapping(suburbs: list[str], channels: list[str]) -> Mapping[str, AssetIn]:
    return {f"{suburb}-{channel}": AssetIn(
        key=[suburb, channel],
        metadata={"suburb": suburb, "channel": channel}) for channel in channels for suburb in suburbs}



start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = dg.MonthlyPartitionsDefinition(start_date=start_date, end_date=end_date)

weekly_partition = dg.WeeklyPartitionsDefinition(start_date=start_date, end_date=end_date)

daily_partition = dg.DailyPartitionsDefinition(start_date=start_date, end_date=end_date)

SUBURBS = [
    "Ararat",
    "Bacchus Marsh"
    "Beaufort",
    "Hoppers Crossing",
    "Horsham",
    "Laverton North",
    "Point Cook",
    "Tarneit",
    "Warrnambool",
    "Werribee",
    "Werribee South",
    "Williams Landing",
    "Wyndham Vale",
]

suburbs_partitions_def = StaticPartitionsDefinition(SUBURBS)

CHANNELS = ["buy", "sold", "rent"]
channels_partitions_def = StaticPartitionsDefinition(CHANNELS)

partitions_mapping = get_partition_mapping(suburbs=SUBURBS, channels=CHANNELS)

# suburbs_channels_partitions_def = DynamicPartitionsDefinition(name="suburb_channel_partition")

suburb_channel_partitions = dg.MultiPartitionsDefinition(
    partitions_defs={"suburb": suburbs_partitions_def, "channel": channels_partitions_def})
