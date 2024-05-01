WITH creator_profile AS 
(
SELECT 
    distinct
    report_info_report_id,
    report_info_created,
    report_info_profile_updated,
    user_profile_user_id,
    user_profile_type,
    user_profile_username,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.influencer_id
        WHEN c.user_profile_type = 'tiktok' then f2.influencer_id
        ELSE NULL 
    END) AS influencer_id,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.date_account_created
        WHEN c.user_profile_type = 'tiktok' then f2.date_account_created
        ELSE NULL
    END) AS date_account_created,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.smileidentity_status
        WHEN c.user_profile_type = 'tiktok' then f2.smileidentity_status
        ELSE NULL 
    END) AS smileidentity_status,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.channel_status_INSTAGRAM
        WHEN c.user_profile_type = 'tiktok' then f2.channel_status_INSTAGRAM
        ELSE NULL 
    END) AS channel_status,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.total_campaigns
        WHEN c.user_profile_type = 'tiktok' then f2.total_campaigns
        ELSE NULL 
    END) AS total_campaigns,
    user_profile_url,
    user_profile_fullname,
    user_profile_is_verified,
    user_profile_is_business,
    user_profile_account_type,
    user_profile_gender,
    user_profile_age_group,
    user_profile_language_code,
    user_profile_language_name,
    user_profile_followers,
    (CASE
        WHEN user_profile_followers BETWEEN 0 AND 250 THEN "NOT QUALIFIED"
        WHEN user_profile_followers BETWEEN 250 AND 5000 THEN "NANO"
        WHEN user_profile_followers BETWEEN 5000 AND 30000 THEN "MICRO"
        WHEN user_profile_followers BETWEEN 30000 AND 100000 THEN "MACRO"
        WHEN user_profile_followers BETWEEN 100000 AND 500000 THEN "MEGA"
        WHEN user_profile_followers >500000 THEN "META"
        WHEN user_profile_followers IS NULL THEN 'No Followers Reported'
    END) AS influencer_type,
    user_profile_posts_count,
    user_profile_engagements,
    user_profile_avg_likes,
    user_profile_avg_comments,
    user_profile_avg_views,
    user_profile_avg_reels_plays,
    user_profile_avg_shares,
    user_profile_avg_saves,
    user_profile_total_likes
FROM `bi-staging-1-309112.wowzi_dbt_prod.potential_premium_creator_iqdata` c 
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
ON lower(c.user_profile_username) = lower(f.username_INSTAGRAM)
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f2
ON lower(c.user_profile_username) = lower(f2.username_TIKTOK)
)

SELECT 
    report_info_report_id,
    report_info_created,
    report_info_profile_updated,
    user_profile_user_id,
    user_profile_type,
    user_profile_username,
    influencer_id,
    (CASE 
        WHEN influencer_id IS NULL THEN false
        ELSE true
    END) is_on_platform,
    date_account_created date_wowzi_acc_created,
    smileidentity_status,
    channel_status channel_approval_status,
    total_campaigns,
    user_profile_url,
    user_profile_fullname,
    user_profile_is_verified,
    user_profile_is_business,
    user_profile_account_type,
    user_profile_gender,
    user_profile_age_group,
    user_profile_language_code,
    user_profile_language_name,
    user_profile_followers,
    influencer_type,
    user_profile_posts_count,
    user_profile_engagements,
    user_profile_avg_likes,
    user_profile_avg_comments,
    user_profile_avg_views,
    user_profile_avg_reels_plays,
    user_profile_avg_shares,
    user_profile_avg_saves,
    user_profile_total_likes
FROM creator_profile