    WITH all_features AS (
        SELECT * FROM {{ source('new_raw', 'raw_listing_features') }}
    )
    SELECT
        DISTINCT
        listing_id,
        feature
    FROM all_features