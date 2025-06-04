WITH all_agencies AS (
    SELECT * FROM {{ source('new_raw', 'raw_agencies') }}
)
SELECT
    agency_id,
    name AS agency_name,
    email AS agency_email,
    address_id,
    website agency_website,
    phone_number AS agency_phone
FROM
    all_agencies