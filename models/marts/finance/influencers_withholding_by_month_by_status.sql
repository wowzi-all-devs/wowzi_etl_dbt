{{ config(tags=["finance"]) }}

WITH payouts AS (
  SELECT 
    influencer_id,
    FORMAT_DATE("%Y-%m-01", payment_eligible_at) AS month,
    status as payment_status,
    SUM(gross_amount) as total_amount
  FROM 
    {{ ref('postgres_stg__influencer_transfers') }}
  GROUP BY 1, 2, 3
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['payouts.influencer_id', 'payouts.month', 'payouts.payment_status']) }} as primary_key,
  payouts.*,
  CONCAT(first_name, " ", last_name) AS name,
  email,
  mobile_number
FROM 
  {{ ref('postgres_stg__influencers') }}
right JOIN
  payouts USING (influencer_id)
WHERE 
  country="KE"
ORDER BY
  month desc