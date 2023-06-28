{{ config(tags=["manual_metrics"]) }}
with metrics as (
select
    distinct
    task_id,
    campaign_id,
    {{ submission_link_channel(submission_link) }} as channel,
    influencer_id,
    submission_link,
    likes,
    comments,
    views,
    shares,
    DATETIME(updated_at) as updated_at
from {{ source('staging', 'raw__manual_metrics') }}),

metrics_and_followers as (
select
  mm.*,
  {{ influencer_type(followers_count) }} as influencer_type,
  cd.followers_count
from metrics mm
LEFT JOIN
  {{ ref('postgres_stg__influencer_channel_data') }}  cd
ON
  LOWER(cd.channel) = LOWER(mm.channel) and  cd.influencer_id=mm.influencer_id
INNER JOIN (
    select task_id, max(updated_at) max_date from metrics group by 1
 )m1 on m1.task_id=mm.task_id and m1.max_date=mm.updated_at

)

SELECT
  metrics_and_followers.campaign_id,
  metrics_and_followers.task_id,
  metrics_and_followers.influencer_id,
  concat(inf.first_name, " ", inf.last_name) as influencer_name,
  metrics_and_followers.channel,
  metrics_and_followers.submission_link,
  metrics_and_followers.likes,
  metrics_and_followers.comments,
  metrics_and_followers.views,
  metrics_and_followers.shares,
  metrics_and_followers.followers_count,
  metrics_and_followers.influencer_type,
  metrics_and_followers.updated_at,

FROM
  metrics_and_followers
LEFT JOIN
  {{ ref('postgres_stg__influencers') }} inf
ON
  inf.influencer_id=metrics_and_followers.influencer_id
ORDER BY
  updated_at
DESC