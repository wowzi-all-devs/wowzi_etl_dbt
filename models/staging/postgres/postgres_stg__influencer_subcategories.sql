SELECT
    id,
    subcategory_id,
    influencer_id
FROM {{ source('staging', 'influencer_subcategories') }}