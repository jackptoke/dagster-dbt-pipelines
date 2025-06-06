from setuptools import find_packages, setup

setup(
    name="dagster_university",
    packages=find_packages(exclude=["dagster_university_tests"]),
    install_requires=[
        "dagster==1.10.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-dbt",
        "dbt-duckdb",
        "dagster-embedded-elt",
        "dagster-duckdb-pandas",
        "geopandas",
        "pandas[parquet]",
        "shapely",
        "matplotlib",
        "smart_open[s3]",
        "s3fs",
        "smart_open",
        "boto3",
        "pyarrow",
        "aiohttp",
        "asyncio",
        "dagster-polars",
        "tenacity"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
