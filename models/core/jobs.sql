SELECT
    id,
    campaign_id,
    influencer_id,
    invitation_status,
    payment_status,
    offer_creation_time,
    offer_expire_time,
    submission_expire_time,
    review_expire_time,
    offer_acception_or_rejection_time,
    creation_time,
    max_influencer_tasks_count,
    max_influencer_amounts_list
FROM {{ source('staging', 'jobs') }}