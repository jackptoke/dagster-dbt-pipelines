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
    WHERE listing_type = 'sold' AND price > 0 AND list_sold_date <> ''
)
SELECT
    DISTINCT
    listing_id,
    price,
    num_bedrooms,
    num_bathrooms,
    num_parking_spaces,
    land_size,
    property_type,
    epoch_ms(list_sold_date::DATE) AS date_id,
    md5(address_id) AS address_id
FROM sold_listings