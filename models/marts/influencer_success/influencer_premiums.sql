WITH max_inf_deliverable_payouts AS 
(
SELECT  
  p.influencer_id, p.social_media_channel, 
  (CASE WHEN p.social_media_channel = 'FACEBOOK' THEN f.influencer_type_FACEBOOK
  WHEN p.social_media_channel = 'INSTAGRAM' THEN f.influencer_type_INSTAGRAM
  WHEN p.social_media_channel = 'TWITTER' THEN f.influencer_type_TWITTER
  WHEN p.social_media_channel = 'TIKTOK' THEN f.influencer_type_TIKTOK
  WHEN p.social_media_channel = 'LINKEDIN' THEN f.influencer_type_LINKEDIN
  END) as influencer_level,
  (CASE WHEN p.social_media_channel = 'FACEBOOK' THEN f.username_FACEBOOK
  WHEN p.social_media_channel = 'INSTAGRAM' THEN f.username_INSTAGRAM
  WHEN p.social_media_channel = 'TWITTER' THEN f.username_TWITTER
  WHEN p.social_media_channel = 'TIKTOK' THEN f.username_TIKTOK
  WHEN p.social_media_channel = 'LINKEDIN' THEN f.username_LINKEDIN
  END) as social_media_username,
  max(p.amount) as max_amount
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` p
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f ON p.influencer_id = f.influencer_id
  WHERE p.social_media_channel IS NOT NULL 
  AND p.social_media_channel <> 'NOT-SUBMITTED'
  AND p.country = 'KE'
  GROUP BY p.influencer_id, p.social_media_channel, f.influencer_type_FACEBOOK,
  f.influencer_type_INSTAGRAM, f.influencer_type_TWITTER, f.influencer_type_TIKTOK,
  f.influencer_type_LINKEDIN, f.username_FACEBOOK, f.username_INSTAGRAM,
  f.username_TWITTER, f.username_TIKTOK, f.username_LINKEDIN
),

premium_by_payout AS
(
SELECT
    influencer_id, 
    social_media_channel, 
    influencer_level, 
    social_media_username,
    max_amount,
    (CASE WHEN influencer_level = 'NANO' AND max_amount >= 2000
    THEN 'YES'
    WHEN influencer_level = 'MICRO' AND max_amount >= 10000
    THEN 'YES'
    WHEN influencer_level = 'MACRO' AND max_amount >= 30000
    THEN 'YES'
    WHEN influencer_level = 'MEGA' AND max_amount >= 50000
    then 'YES'
    WHEN influencer_level = 'META' AND max_amount >= 100000
    THEN 'YES'
    END) as premium_tag
FROM max_inf_deliverable_payouts),

premium_inf_with_content_quality AS
(SELECT
    influencer_id, 
    social_media_channel, 
    influencer_level,
    social_media_username,
    premium_tag
FROM premium_by_payout pp 
    WHERE premium_tag = 'YES'
    AND influencer_id in 
    (SELECT 
    influencer_id
    FROM `bi-staging-1-309112.wowzi_airbyte.influencer_hidden_tags` 
    WHERE hidden_tag IN ('HIGH_CONTENT_QUALITY', 'AVERAGE_CONTENT_QUALITY')))

SELECT 
    influencer_id,
    social_media_channel, 
    influencer_level,
    social_media_username,
    premium_tag 
FROM premium_inf_with_content_quality
    ORDER BY influencer_id