WITH cleaned AS (
  SELECT
    influencer_id,
    created_at,
    processed_at,
    end_date,
    status,
    verification_status,
    LOWER(platform) AS platform,
    campaign_id,
    task_id,
    /* Make null values = 0, convert negative vals to positive */
    ABS(COALESCE(reach_delta, 0)) AS reach_delta,
    ABS(COALESCE(post_impressions_delta, 0)) AS post_impressions_delta,
    ABS(COALESCE(views_delta, 0)) AS views_delta,
    ABS(COALESCE(comments_delta, 0)) AS comments_delta,
    ABS(COALESCE(likes_delta, 0)) AS likes_delta,
    ABS(COALESCE(shares_delta, 0)) AS shares_delta,
    ABS(COALESCE(saved_delta, 0)) AS saved_delta,
    ABS(COALESCE(total_interactions_delta, 0)) AS total_interactions_delta,
    ABS(COALESCE(post_impressions_unique_delta, 0)) AS post_impressions_unique_delta,
    ABS(COALESCE(post_reactions_like_total_delta, 0)) AS post_reactions_like_total_delta,
    ABS(COALESCE(engagement_rate, 0)) AS engagement_rate
 from 
 {{ source('staging', 'campaign_report_metrics_daily') }}
),
per_platform AS (
  SELECT
    influencer_id,
    created_at,
    processed_at,
    extract(week(MONDAY) from date(processed_at)) AS week_number,
    end_date,
    status,
    verification_status,
    platform,
    campaign_id,
    task_id,
    CASE
      WHEN platform = 'twitter'  THEN views_delta                         /* Twitter: reach = views */
      WHEN platform = 'tiktok'   THEN post_impressions_delta              /* TikTok: reach = impressions */
      WHEN platform = 'facebook' THEN post_impressions_unique_delta       /* Facebook: reach = unique impressions */
      ELSE reach_delta
    END AS reach_delta,
    post_impressions_delta,
    CASE
      WHEN platform = 'tiktok'   THEN post_impressions_delta              /* TikTok: views = impressions */
      WHEN platform = 'facebook' THEN post_impressions_delta              /* Facebook: views = impressions */
      WHEN platform = 'twitter'  THEN views_delta                         /* Twitter already has views */
      WHEN platform = 'instagram'
           AND status = 'new'
           AND views_delta = 0
           AND reach_delta > 0
        THEN reach_delta                                               /* IG: if new & views=0 & reach>0 → views = reach */
      ELSE views_delta
    END AS views_delta,
    CASE
      WHEN platform = 'facebook' THEN post_reactions_like_total_delta
      ELSE likes_delta
    END AS likes_delta,
    comments_delta,
    shares_delta,
    saved_delta,
    CASE
      WHEN platform IN ('twitter', 'tiktok', 'instagram') THEN
        (CASE
           WHEN platform = 'facebook' THEN 0
           ELSE
             likes_delta
         END)
        + comments_delta
        + shares_delta
      WHEN platform = 'facebook' THEN
        COALESCE(post_reactions_like_total_delta,  0)
      ELSE total_interactions_delta
    END AS total_interactions_delta,
    engagement_rate,

    /*
    Optional: total_interactions (only populated when status = 'new')
    mirrors the Python:
    if status == "new": record["total_interactions"] = total_interactions_delta
    */
    CASE
      WHEN status = 'new' THEN
        CASE
          WHEN platform IN ('twitter', 'tiktok', 'instagram') THEN
            likes_delta + comments_delta + shares_delta
          WHEN platform = 'facebook' THEN
            COALESCE(post_reactions_like_total_delta,  0)
          ELSE total_interactions_delta
        END
      ELSE NULL
    END AS total_interactions
  FROM cleaned
)

SELECT *
FROM per_platform





