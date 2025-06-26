{{
    config(
        materialized = 'incremental',
        unique_key = 'listing_id',
        on_schema_change = 'sync_all_columns'
    )
 }}
WITH sold_listings AS (
    SELECT listing_id, listing_title, price, list_sold_date,
           num_bedrooms, num_bathrooms, num_parking_spaces,
           land_size, property_type, listing_type, address_id
    FROM {{ ref('stg_listings') }}
    WHERE listing_type = 'sold' AND price > 0 AND list_sold_date <> ''
),
    all_addresses AS (
        SELECT address_id, street_address,
               suburb, state, postcode, latitude,
               longitude
        FROM {{ ref('stg_addresses') }}
    )
SELECT
    L.listing_id,
    L.listing_title AS title,
    L.price,
    L.property_type,
    L.num_bedrooms AS bedrooms,
    L.num_bathrooms AS bathrooms,
    L.num_parking_spaces AS parking,
    L.land_size land,
    L.listing_type AS channel,
    L.list_sold_date AS sold_date,
    EXTRACT(YEAR FROM DATE(L.list_sold_date)) AS year,
    A.street_address AS address,
    LOWER(A.suburb) AS suburb,
    LOWER(A.state) AS state,
    A.postcode,
    A.latitude,
    A.longitude
FROM sold_listings L
JOIN all_addresses A ON A.address_id = L.address_id
