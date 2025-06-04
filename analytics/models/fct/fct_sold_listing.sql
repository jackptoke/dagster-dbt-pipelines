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
    WHERE listing_type = 'sold' AND price > 0
)
SELECT
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