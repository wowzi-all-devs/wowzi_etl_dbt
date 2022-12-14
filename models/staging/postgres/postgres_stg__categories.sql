SELECT
    id,
    enum_value,
    min_count,
    max_count
FROM {{ source('staging', 'categories') }}