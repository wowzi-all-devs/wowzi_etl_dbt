SELECT
    id,
    parent_category_id,
    enum_value
FROM {{ source('core', 'subcategories') }}