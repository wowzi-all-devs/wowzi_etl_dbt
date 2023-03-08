SELECT
    {{ dbt_utils.surrogate_key(['company_id', 'merchant_id']) }} as primary_key,
    company_id,
    merchant_id,
    merchant_role,
    created_at,
    updated_at
FROM {{ source('staging', 'company_merchants') }}