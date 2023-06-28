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
from {{ source('staging', 'raw__manual_metrics') }})

select mm.*, {{ influencer_type(followers_count)}}as influencer_type,  cd.followers_count
 from metrics mm LEFT JOIN
  {{ ref('postgres_stg__influencer_channel_data') }}  cd
ON
  LOWER(cd.channel) = LOWER(mm.channel) and  cd.influencer_id=mm.influencer_id
 inner join (
    select task_id, max(updated_at) max_date from metrics group by 1
 )m1 on m1.task_id=mm.task_id and m1.max_date=mm.updated_at
ORDER BY
  updated_at
 desc