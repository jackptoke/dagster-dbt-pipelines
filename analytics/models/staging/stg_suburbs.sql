WITH all_suburbs AS (
    SELECT DISTINCT * FROM {{ source('new_raw', 'raw_suburbs') }}
)
SELECT
    ssc_code AS suburb_id,
    suburb AS suburb_name,
    urban_area,
    postcode,
    state AS state_code,
    state_name,
    type AS suburb_type,
    local_goverment_area AS local_government_area,
    statistic_area,
    elevation,
    population,
    median_income,
    sqkm,
    lat,
    lng,
    timezone
FROM
    all_suburbs