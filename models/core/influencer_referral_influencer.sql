SELECT
    influencer_id,
    referred_by_influencer_id,
    creation_time
FROM {{ source('core', 'influencer_referral_influencer') }}