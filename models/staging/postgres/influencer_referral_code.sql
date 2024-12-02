SELECT
    influencer_id,
    code,
    enabled as is_enabled,
    creation_time
FROM {{ source('staging', 'influencer_referral_code') }} 