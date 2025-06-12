{{
    config(
        materialized = 'incremental',
        unique_key = ['listing_id', 'agent_id'],
        on_schema_change = 'sync_all_columns'
    )
}}
WITH all_listings AS (
    SELECT DISTINCT *  FROM {{ ref('fct_sold_listing') }}
),
    all_listing_agents AS (
        SELECT DISTINCT *  FROM {{ ref('stg_listing_agents') }}
    )
SELECT
    DISTINCT
    L.listing_id,
    A.agent_id
FROM all_listings L
JOIN all_listing_agents A
ON A.listing_id = L.listing_id