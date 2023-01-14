SELECT
    id,
    country,
    influencer_id,
    external_id as tax_external_id,
    created_at,
    updated_at
FROM {{ source('staging', 'influencer_taxes_info') }}