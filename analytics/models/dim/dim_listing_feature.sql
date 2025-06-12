{{
    config(
        materialized = 'incremental',
        unique_key=['listing_id', 'feature'],
        on_schema_change='sync_all_columns'
    )
 }}
WITH all_listing_features AS (
    SELECT * FROM {{ ref('stg_listing_features') }}
)
SELECT DISTINCT listing_id, feature FROM all_listing_features