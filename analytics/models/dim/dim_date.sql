{{
    config(
        materialized = 'incremental',
        unique_key = 'date_id',
        on_schema_change = 'sync_all_columns'
    )
 }}
WITH all_dates AS (
    SELECT DISTINCT list_sold_date AS new_date FROM {{ source('cleansed_data', 'staging_listings') }} WHERE list_sold_date is not null AND list_sold_date != ''
    UNION
    SELECT DISTINCT date_available AS new_date FROM {{ source('cleansed_data', 'staging_rental_listings') }} WHERE date_available is not null AND date_available != ''
)
select
    epoch_ms(new_date::DATE) AS date_id,
    new_date AS date_value,
    year(new_date::DATE) AS 'year',
    month(new_date::DATE) AS 'month',
    day(new_date::DATE) AS 'day',
    dayofweek(new_date::DATE) AS 'day_of_week',
    week(new_date::DATE) AS 'week',
    quarter(new_date::DATE) AS 'quarter'
from all_dates