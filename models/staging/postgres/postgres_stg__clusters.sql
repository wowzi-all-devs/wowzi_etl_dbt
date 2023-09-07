SELECT
    id as cluster_id,
    name as cluster_name,
    company_id
    created_at,
    updated_at
FROM {{ source('staging', 'clusters') }}