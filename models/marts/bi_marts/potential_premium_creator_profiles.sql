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
        WHEN c.user_profile_type = 'instagram' then f.first_name||' '||f.last_name
        WHEN c.user_profile_type = 'tiktok' then f2.first_name||' '||f2.last_name
        ELSE NULL 
    END) AS inf_full_name,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.age_range
        WHEN c.user_profile_type = 'tiktok' then f2.age_range
        ELSE NULL 
    END) AS inf_age_range,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.gender
        WHEN c.user_profile_type = 'tiktok' then f2.gender
        ELSE NULL 
    END) AS inf_gender,
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
        WHEN c.user_profile_type = 'tiktok' then f2.channel_status_TIKTOK
        ELSE NULL 
    END) AS channel_status,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.job_eligibility
        WHEN c.user_profile_type = 'tiktok' then f2.job_eligibility
        ELSE NULL 
    END) AS job_eligibility,
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
    user_profile_engagement_rate,
    (CASE 
        WHEN user_profile_engagement_rate < 1 THEN '<1'
        WHEN user_profile_engagement_rate >= 1  AND user_profile_engagement_rate < 5 THEN '>=1 <5'
        WHEN user_profile_engagement_rate >= 5  AND user_profile_engagement_rate < 10 THEN '>=5 <10'
        WHEN user_profile_engagement_rate >= 10  AND user_profile_engagement_rate < 20 THEN '>=10 <20'
        WHEN user_profile_engagement_rate >= 20  AND user_profile_engagement_rate < 50 THEN '>=20 <50'
        WHEN user_profile_engagement_rate >= 50 THEN '>=50'
    END) engagement_rate_bucket,
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
    p.report_info_report_id,
    p.report_info_created,
    p.report_info_profile_updated,
    p.user_profile_user_id,
    p.user_profile_type,
    p.user_profile_username,
    p.influencer_id,
    (CASE 
        WHEN p.influencer_id IS NULL THEN false
        ELSE true
    END) is_on_platform,
    p.date_account_created date_wowzi_acc_created,
    p.smileidentity_status,
    p.channel_status channel_approval_status,
    p.job_eligibility,
    p.total_campaigns,
    p.user_profile_url,
    (CASE 
        WHEN p.influencer_id IS NULL THEN p.user_profile_fullname
        ELSE p.inf_full_name
    END) user_profile_fullname,
    p.user_profile_is_verified,
    p.user_profile_is_business,
    p.user_profile_account_type,
    (CASE 
        WHEN p.influencer_id IS NULL THEN p.inf_gender
        ELSE p.user_profile_gender
    END) user_profile_gender,
    (CASE 
        WHEN p.influencer_id IS NULL THEN p.inf_age_range
        ELSE p.user_profile_age_group
    END) user_profile_age_group,
    p.user_profile_language_code,
    p.user_profile_language_name,
    p.user_profile_followers,
    p.influencer_type,
    p.user_profile_posts_count,
    p.user_profile_engagements,
    p.user_profile_engagement_rate,
    p.engagement_rate_bucket,
    p.user_profile_avg_likes,
    p.user_profile_avg_comments,
    p.user_profile_avg_views,
    p.user_profile_avg_reels_plays,
    p.user_profile_avg_shares,
    p.user_profile_avg_saves,
    p.user_profile_total_likes,
    ir.ranking_score instagram_rank,
    tr.ranking_score tiktok_rank
FROM creator_profile p
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.creator_xgboost_ranks_instagram` ir 
ON p.user_profile_user_id = ir.user_profile_user_id AND LOWER(user_profile_type) = 'instagram'
LEFT JOIN  `bi-staging-1-309112.wowzi_dbt_prod.creator_xgboost_ranks_tiktok` tr
ON p.user_profile_user_id = ir.user_profile_user_id AND LOWER(user_profile_type) = 'tiktok'
