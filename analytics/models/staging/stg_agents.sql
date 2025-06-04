WITH all_agents AS (
    SELECT * FROM {{ source('new_raw', 'raw_agents') }}
)
SELECT
    email as agent_id,
    full_name as agent_name,
    job_title as agent_title,
    website as agent_website,
    phone_number as agent_phone,
    mobile_number as agent_mobile,
    agency_id
FROM all_agents