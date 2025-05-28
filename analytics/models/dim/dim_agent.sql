{{
    config(
        materialized = 'incremental',
        unique_key = 'agent_id',
        on_schema_change = 'sync_all_columns'
    )
}}
WITH all_agents AS (
    SELECT * FROM {{ ref('stg_agents') }}
),
all_agencies AS (
    SELECT * FROM {{  ref('stg_agencies') }} WHERE agency_email != ''
)
SELECT
    md5(agent_id) agent_id,
    agent_name,
    agent_title,
    agent_website,
    agent_phone,
    agent_mobile,
    B.agency_id
FROM all_agents A
LEFT JOIN all_agencies B ON B.agency_email = A.agency_id
