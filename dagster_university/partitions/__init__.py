import dagster as dg
from dagster import StaticPartitionsDefinition
from ..assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = dg.MonthlyPartitionsDefinition(start_date=start_date, end_date=end_date)

weekly_partition = dg.WeeklyPartitionsDefinition(start_date=start_date, end_date=end_date)

daily_partition = dg.DailyPartitionsDefinition(start_date=start_date, end_date=end_date)

SUBURBS = [
    "Ararat",
    "Beaufort",
    "Hoppers Crossing",
    "Horsham",
    "Werribee",
    "Tarneit",
    "Laverton North",
    "Williams Landing",
    "Wyndham Vale",
    "Point Cook",
    "Werribee South",
    "Cocoroc",
    "Quandong",
    "Mambourin",
    "Bacchus Marsh"
]

suburbs_partitions_def = StaticPartitionsDefinition(SUBURBS)

CHANNELS = ["buy", "sold", "rent"]
channels_partitions_def = StaticPartitionsDefinition(CHANNELS)
