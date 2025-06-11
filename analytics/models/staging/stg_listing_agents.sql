{{
    config(
        unique_key = ['listing_id', 'agent_id'],
        on_schema_change = 'sync_all_columns'
    )
}}
WITH all_listing_agents AS (
        SELECT * FROM {{ source('new_raw', 'raw_listing_agents') }}
    )
    SELECT
        DISTINCT
        listing_id,
        agent_id
    FROM all_listing_agents