import dagster as dg
from dagster import StaticPartitionsDefinition, DynamicPartitionsDefinition
from ..assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = dg.MonthlyPartitionsDefinition(start_date=start_date, end_date=end_date)

weekly_partition = dg.WeeklyPartitionsDefinition(start_date=start_date, end_date=end_date)

daily_partition = dg.DailyPartitionsDefinition(start_date=start_date, end_date=end_date)

SUBURBS = [
    # "Ararat",
    # "Bacchus Marsh"
    # "Beaufort",
    # "Hoppers Crossing",
    "Werribee",
    # "Wyndham Vale",
    # "Horsham",
    # "Tarneit",
    # "Laverton North",
    # "Williams Landing",
    # "Point Cook",
    # "Werribee South",
    # "Cocoroc",
    # "Quandong",
    # "Mambourin",
]

suburbs_partitions_def = StaticPartitionsDefinition(SUBURBS)

CHANNELS = ["buy", "sold", "rent"]
channels_partitions_def = StaticPartitionsDefinition(CHANNELS)

# suburbs_channels_partitions_def = DynamicPartitionsDefinition(name="suburb_channel_partition")

suburb_channel_partitions = dg.MultiPartitionsDefinition(
    partitions_defs={"suburb": suburbs_partitions_def, "channel": channels_partitions_def})
