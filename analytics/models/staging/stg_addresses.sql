  WITH all_addresses AS (
    SELECT * FROM {{ source('new_raw', 'raw_addresses') }}
)
SELECT
    address_id,
    street_address,
    suburb,
    state,
    postcode,
    latitude,
    longitude
FROM all_addresses