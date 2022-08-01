WITH
  task_data AS(
  SELECT
    campaign_id,
    COUNT(id) AS total_tasks,
    COUNTIF(first_verification_status="APPROVED") quality_verified_tasks,
    COUNTIF(second_verification_status="APPROVED") problematic_verified_tasks,
    COUNTIF(third_verification_status="APPROVED") time_verified_tasks,
  FROM
    {{ ref('influencer_tasks') }}
  GROUP BY
    1 
)
SELECT
  c.campaign_id,
  c.name as campaign_name,
  c.campaign_creation_date,
  c.start_date,
  c.end_date,
  c.currency,
  c.budget,
  ce.budget_spent,
  c.bonus_amount,
  c.canceled,
  c.canceled_timestamp,
  ce.influencers_invited_count,
  ce.influencers_accepted_count,
  ce.company_name,
  task_data.total_tasks,
  task_data.quality_verified_tasks,
  task_data.problematic_verified_tasks,
  task_data.time_verified_tasks,
  adv.country
FROM
  {{ ref('campaigns') }} c
LEFT JOIN
  task_data
ON
  c.campaign_id=task_data.campaign_id
LEFT JOIN
  {{ ref('campaign_expenditure') }} ce
ON
  c.campaign_id=CAST(ce.campaign_id AS integer)
LEFT JOIN
  {{ ref('dims_advertisers') }} adv
ON
  c.merchant_id=adv.advertiser_id
ORDER BY
  c.campaign_id DESC