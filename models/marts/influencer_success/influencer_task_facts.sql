{{ config(tags=["influencer_success"]) }}

SELECT
  inft.influencer_id,
  c.company_id,
  inft.task_id,
  inft.job_id,
  inft.campaign_id,
  ct.channel,
  submission_link,
  submission_link_add_time,
  submission_status,
  first_verification_status,
  second_verification_status,
  third_verification_status,
  payment_amount_list,
  top_post,
  inft.creation_time AS task_creation_time,
  country
FROM
  {{ ref('postgres_stg__influencer_tasks') }} inft
LEFT JOIN
  {{ ref('postgres_stg__influencers') }}
USING
  (influencer_id)
LEFT JOIN
  {{ ref('postgres_stg__campaigns') }} c
ON
  inft.campaign_id=c.campaign_id
LEFT JOIN
  {{ ref('postgres_stg__campaign_tasks') }} ct
ON
  inft.campaign_task_id=ct.id