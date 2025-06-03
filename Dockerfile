FROM python:3.12-slim
LABEL authors="jacktoke"

ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip

RUN pip install \
    dagster \
    dagster-cloud \
    dagster-duckdb \
    dagster-dbt \
    dagster-postgres \
    dagster-docker \
    dbt-duckdb \
    dagster-embedded-elt \
    dagster-duckdb-pandas \
    geopandas \
    pandas[parquet] \
    shapely \
    matplotlib \
    smart_open[s3] \
    s3fs \
    smart_open \
    boto3 \
    pyarrow \
    aiohttp \
    asyncio \
    dagster-polars \
    tenacity

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

COPY . /opt/dagster/app

EXPOSE 4000

CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_university"]