{{
    config(
        materialized = 'incremental',
        unique_key=['suburb_id',],
        on_schema_change='sync_all_columns'
    )
 }}
WITH all_aus_suburbs AS (
    SELECT * FROM {{ ref('stg_suburbs') }}
)
SELECT
    suburb_id,
    suburb_name,
    state_name,
    state_code,
    population,
    median_income,
    sqkm,
    lat AS latitude,
    lng AS longitude,
    timezone
FROM
    all_aus_suburbs