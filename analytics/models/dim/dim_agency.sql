{{
    config(
        materialized = 'incremental',
        unique_key = 'agency_id',
        on_schema_change = 'sync_all_columns'
    )
 }}
WITH all_agencies AS (
    SELECT * FROM {{  ref('stg_agencies') }}
)
SELECT
    DISTINCT
    agency_id,
    agency_name,
    agency_email,
    agency_website,
    agency_phone,
    md5(address_id) AS address_id
FROM all_agencies