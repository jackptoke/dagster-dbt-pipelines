from dagster import Config


class ListingOpConfig(Config):
    suburb: str
    channel: str
