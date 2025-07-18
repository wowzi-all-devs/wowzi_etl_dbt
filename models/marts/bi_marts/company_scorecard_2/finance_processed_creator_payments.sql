SELECT
  influencer_id,
  currency,
  country,
  date(payment_eligible_at) payment_eligible_at,
  date(processed_date) processed_date,
  case when 
    lower(fine_payment_status) in ('successful', 'manual') 
    and 
    date(processed_date) > date(payment_eligible_at)
     then 
    date_diff(date(processed_date), date(payment_eligible_at), day) 
    -- DATE_DIFF(DATE(processed_date), DATE(payment_eligible_at), DAY) 
    else null
    end as payment_diff_in_days,
  fine_payment_status,
  concat('Q',EXTRACT(QUARTER FROM payment_eligible_at)) AS qtr,
  EXTRACT(YEAR FROM payment_eligible_at) AS year,
  concat('Q',EXTRACT(QUARTER FROM payment_eligible_at), "-", EXTRACT(YEAR FROM payment_eligible_at)) qtr_yr,
  DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), QUARTER), WEEK) + 1 week_of_qtr,
  CASE
    WHEN lower(fine_payment_status) in ('successful', 'manual', 'new', 'completed')
         AND processed_date <= DATE_ADD(date(payment_eligible_at), INTERVAL 2 DAY)
    THEN 1
    ELSE 0
  END AS paid_within_2_days_flag
  from bi-staging-1-309112.wowzi_dbt_prod.creator_payments_information
  where fine_payment_status <> 'Awaiting payment'
   order by payment_eligible_at desc

