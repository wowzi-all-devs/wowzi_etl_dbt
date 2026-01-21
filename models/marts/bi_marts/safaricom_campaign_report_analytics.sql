
with poc_inf as
(
select 
influencer_id
from 
{{ source('staging', 'gsheets__safaricom_poc_influencers')}}
-- `bi-staging-1-309112.wowzi_dbt_prod.gsheets__safaricom_poc_influencers`
),
safaricom_camp as
(
select 
a.influencer_id,
b.task_id,
b.processed_at,
b.platform,
b.week_number,
b.reach_delta reach,
b.post_impressions_delta post_impressions,
b.views_delta views,
b.likes_delta likes,
b.comments_delta comments,
b.shares_delta shares,
b.total_interactions_delta total_interactions,
/* Week start for the record */
  DATE_TRUNC(DATE(b.processed_at), WEEK(MONDAY)) AS record_week_start,

/* Week start for "this" calendar week (current week) */
  DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))       AS curr_week_start,

/* "This week" = previous full week (last Mon–Sun) */
  CASE 
    WHEN DATE_TRUNC(DATE(b.processed_at), WEEK(MONDAY))
        = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 7 DAY)
    THEN TRUE 
    ELSE NULL
  END AS is_this_week,        

/* "Last week" = week from 2 weeks ago (Mon–Sun) */
  CASE 
    WHEN DATE_TRUNC(DATE(b.processed_at), WEEK(MONDAY))
        = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 14 DAY)
    THEN TRUE 
    ELSE NULL
  END AS is_last_week           

from 
poc_inf a
left join
bi-staging-1-309112.wowzi_dbt_prod.postgres_stg__campaign_report_metrics_daily b
on a.influencer_id = b.influencer_id
and b.campaign_id in (10375,10543,10510, 10566, 10530)
)
select *,
row_number() over (order by processed_at desc) rnk
 from 
safaricom_camp
-- where 
-- processed_at < curr_week_start. --- exclude the filter and then use the 'is_this_week' field to flag on the dashboard
