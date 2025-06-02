{{
    config(
        materialized = 'incremental',
        unique_key = 'listing_id',
        on_schema_change = 'sync_all_columns'
    )
 }}
WITH sold_listings AS (
    SELECT *
    FROM {{ source('cleansed_data', 'staging_listings') }}
    WHERE listing_type = 'buy'
)
SELECT
    listing_id,
    price,
    num_bedrooms,
    num_bathrooms,
    num_parking_spaces,
    ad_lower_price,
    ad_upper_price,
    land_size,
    md5(address_id) AS address_id
FROM sold_listings