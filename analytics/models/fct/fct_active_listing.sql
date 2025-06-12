{{
    config(
        materialized = 'incremental',
        unique_key = 'listing_id',
        on_schema_change = 'sync_all_columns'
    )
 }}
WITH sold_listings AS (
    SELECT *
    FROM {{ ref('stg_listings') }}
    WHERE listing_type = 'buy'
)
SELECT
    DISTINCT
    listing_id,
    price,
    num_bedrooms,
    num_bathrooms,
    num_parking_spaces,
    ad_lower_price,
    ad_upper_price,
    land_size,
    property_type,
    md5(address_id) AS address_id
FROM sold_listings