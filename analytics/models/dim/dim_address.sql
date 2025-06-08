{{
    config(
        materialized = 'incremental',
        unique_key = 'address_id',
        on_schema_change = 'sync_all_columns'
    )
 }}
WITH all_addresses AS (
    SELECT * FROM {{ ref('stg_addresses') }}
)
SELECT
    DISTINCT
    md5(address_id) AS address_id,
    street_address,
    suburb,
    state,
    postcode,
    latitude,
    longitude
FROM all_addresses