SELECT
    id,
    invitation_code,
    name,
    description,
    company_id,
    deleted,
    creation_time
FROM {{ source('staging', 'influencer_groups') }}
WHERE name is not null