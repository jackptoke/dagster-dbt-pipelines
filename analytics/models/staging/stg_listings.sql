    WITH all_listings AS (
        SELECT * FROM {{ source('new_raw', 'raw_listings') }}
    )
    SELECT
        DISTINCT
        listing_id,
        listing_title,
        property_type,
        listing_type,
        construction_status,
        price,
        ad_lower_price,
        ad_upper_price,
        num_bedrooms,
        num_bathrooms,
        num_parking_spaces,
        land_size,
        listing_description,
        listing_status,
        list_sold_date,
        agency_id,
        address_id
    FROM all_listings