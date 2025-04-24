{{ config(tags=["business_development"]) }}


WITH advertisers_grouped AS (
  SELECT
    merchant_id,
    currency,
    EXTRACT(MONTH FROM DATE(TIMESTAMP(campaign_start_date))) AS campaign_start_month,
    SUM(campaign_budget) AS total_spent
  FROM 
    {{ ref('campaign_expenditure') }}
  WHERE
    canceled = FALSE
  GROUP BY
    merchant_id, currency, campaign_start_month
),

deduped_dim_advertisers AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY advertiser_id 
             ORDER BY date_account_created DESC -- or created_at if updated_at doesn't exist
           ) AS row_num
    FROM {{ ref('dim_advertisers') }}
  )
  WHERE row_num = 1
),

advertisers_with_name AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['merchant_id', 'campaign_start_month', 'currency']) }} AS primary_key,
    ag.merchant_id,
    adv.company_name,
    adv.country AS country,
    ag.campaign_start_month,
    ag.total_spent,
    ag.currency
  FROM advertisers_grouped ag
  LEFT JOIN deduped_dim_advertisers adv
    ON ag.merchant_id = adv.advertiser_id
)

SELECT *
FROM advertisers_with_name
ORDER BY total_spent DESC

-- WITH advertisers_grouped as (
--   SELECT
--     merchant_id,
--     currency,
--     extract(month from date(timestamp(campaign_start_date))) as campaign_start_month,
--     SUM(campaign_budget) as total_spent
--   FROM 
--     {{ ref('campaign_expenditure') }}
--   WHERE
--     canceled=False
--   GROUP BY
--     1, 2, 3
-- ),
-- advertisers_with_name as (
--   SELECT
--     {{ dbt_utils.generate_surrogate_key(['merchant_id', 'campaign_start_month', 'currency', 'company_name']) }} as primary_key,
--     advertisers_grouped.merchant_id,
--     adv.company_name,
--     adv.country as country,
--     advertisers_grouped.campaign_start_month,
--     advertisers_grouped.total_spent,
--     advertisers_grouped.currency
--   FROM
--     advertisers_grouped
--   LEFT JOIN
--     {{ ref('dim_advertisers') }} adv
--   ON 
--     advertisers_grouped.merchant_id=adv.advertiser_id
-- )
-- SELECT *
-- FROM 
--   advertisers_with_name
-- ORDER BY
--   total_spent desc