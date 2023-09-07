SELECT
    id,
    cluster_id,
    country,
    created_at,
    updated_at
FROM {{ source('staging', 'cluster_country') }}