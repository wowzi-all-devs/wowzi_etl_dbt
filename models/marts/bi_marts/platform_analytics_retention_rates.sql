-- ================================================
-- Month-over-month retention (rolling, no hard-coding)
-- Compares: previous full month  vs  last full month
-- Example (if today is 2025-11-12):
--   prev month = 2025-09
--   curr month = 2025-10
-- ================================================

-- 1) Work out the four dates dynamically.
-- prev_start: first day of the month before last
-- prev_end  : last day of the month before last
-- curr_start: first day of last month
-- curr_end  : last day of last month
-- DECLARE prev_start DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH);
-- DECLARE prev_end   DATE DEFAULT DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY);
-- DECLARE curr_start DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH);
-- DECLARE curr_end   DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY);

-- 2) Define which events “count” as activity.
-- Keep this list short and meaningful so the metric reflects real usage.
WITH activity_events AS (
  SELECT event FROM UNNEST([
    'User Login',
    'Campaign Created',
    'Campaign Drafted',
    'Brief Created',
    'Brief Updated',
    'Brand Created',
    'Company Created',
    'Creator Discovery Viewed',
    'Campaign Report Viewed',
    'Teammate Invited',
    'Token Purchase',
    'Top Up Clicked',
    'Social Media Profile Link Attempt Fb',
    'Social Media Profile Link Attempt Insta'
  ]) AS event
),

-- 3) Users who did at least one “activity” in the previous full month.
prev_active AS (
  SELECT DISTINCT e.user_id
  FROM bi-staging-1-309112.wowzi_dbt_prod.platform_analytics e
  JOIN activity_events a
    ON a.event = e.fine_event_name
  WHERE DATE(e.created) 
  BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH) 
  AND DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
  -- Optional: exclude bots/test users here
  -- AND e.user_id NOT IN UNNEST(['test1','test2'])
),

-- 4) Users who did at least one “activity” in the last full month.
curr_active AS (
  SELECT DISTINCT e.user_id
  FROM bi-staging-1-309112.wowzi_dbt_prod.platform_analytics e
  JOIN activity_events a
    ON a.event = e.fine_event_name
  WHERE DATE(e.created) 
  BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) 
  AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) 
),

-- 5) Users who showed up in both months = retained.
retained AS (
  SELECT user_id FROM prev_active
  INTERSECT DISTINCT
  SELECT user_id FROM curr_active
)

-- 6) Final counts and the retention rate.
SELECT
  DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH) AS prev_start,
  DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS prev_month_end,
  DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS curr_month_start,
  DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) AS curr_month_end,

  (SELECT COUNT(*) FROM prev_active)  AS prev_active_users,
  (SELECT COUNT(*) FROM curr_active)  AS curr_active_users,
  (SELECT COUNT(*) FROM retained)     AS retained_users,

  SAFE_DIVIDE(
    (SELECT COUNT(*) FROM retained),
    (SELECT COUNT(*) FROM prev_active)
  ) AS retention_rate