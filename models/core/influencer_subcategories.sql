SELECT
    id,
    subcategory_id,
    influencer_id
FROM {{ source('core', 'influencer_subcategories') }}