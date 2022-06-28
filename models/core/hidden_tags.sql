SELECT
    hidden_tag,
    en_label,
    update_time,
    creation_time
FROM {{ source('core', 'hidden_tags') }}