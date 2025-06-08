WITH rental_listings AS (
    SELECT * FROM {{ source('cleansed_data', 'staging_rental_listings') }}
),
agencies AS (
    SELECT * FROM {{ ref('dim_agency') }}
)
SELECT
    DISTINCT
    listing_id,
    price,
    price_period,
    bond,
    num_bedrooms,
    num_bathrooms,
    num_parking_spaces,
    property_type,
    epoch_ms(date_available::DATE) AS date_id,
    A.agency_id,
    md5(R.address_id) AS address_id,
FROM rental_listings R
LEFT JOIN agencies A ON A.agency_email = R.agency_id
