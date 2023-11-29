SELECT
    id,
    cluster_id,
    company_id,
    created_at,
    updated_at
FROM {{ source('staging', 'cluster_company') }}