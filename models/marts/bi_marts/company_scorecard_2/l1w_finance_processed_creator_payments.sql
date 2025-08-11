
/*
creators due to be paid in the last one week (counting 
from Thursdays) that were paid within 2 days of their due date
*/

WITH a AS 
(
SELECT
  payment_id,
  influencer_id,
  task_id,
  LOWER(fine_payment_status) AS fine_payment_status,
  payment_eligible_at,
  processed_date,
  -- extracyed previous friday's date to use as a baseline for 30days from last friday
  DATE_TRUNC(CURRENT_DATE() - INTERVAL 7 DAY, WEEK(MONDAY)) + INTERVAL 4 DAY AS prev_friday
FROM bi-staging-1-309112.wowzi_dbt_prod.creator_payments_information
),
b as 
(
select 
* 
from a
where 
DATE(payment_eligible_at)
BETWEEN DATE_SUB(prev_friday, INTERVAL 30 DAY) AND prev_friday
)
, 
c as
( 
 SELECT
    b.*,
    -- -- Get today's date
    -- CURRENT_DATE() AS current_date,
    -- -- Get the day of the week for today
    -- EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) AS current_dow,
    -- -- Calculate the last Thursday
    -- DATE_SUB(CURRENT_DATE(), 
    --     INTERVAL CASE 
    --       -- If today is Thursday (5) or later (Friday 6, Saturday 7)
    --       WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) > 5 THEN 
    --         -- Subtract the number of days since last Thursday
    --         (EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) - 5)
    --       ELSE 
    --         -- If today is Sunday (1) to Wednesday (4), go back to last Thursday
    --         (EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 2)
    --     END DAY) 
    -- AS last_thursday,
    -- -- Calculate this week's Thursday
    -- DATE_ADD(CURRENT_DATE(), INTERVAL CASE 
    --   -- If today is Sunday (1) to Thursday (5)
    --   WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) <= 5 THEN 
    --     -- Add the number of days to reach this Thursday
    --     (5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()))
    --   ELSE 
    --     -- If today is Friday (6) or Saturday (7), go to next Thursday
    --     (12 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()))
    -- END DAY) AS current_thursday

    -- Thursdays relative to the global prev_friday
    DATE_SUB(prev_friday, INTERVAL 8 DAY) AS last_thursday,     -- e.g., 2025-07-24
    DATE_SUB(prev_friday, INTERVAL 1 DAY) AS current_thursday 

  FROM b
),
last_one_week as
(
select
payment_id,
influencer_id,
task_id,
fine_payment_status,
last_thursday,
current_thursday,
date(payment_eligible_at) payment_eligible_at,
date(processed_date) processed_date,

case when 
  lower(fine_payment_status) in ('successful - backoffice', 'successful - marked') 
  and 
  /*
  when the payment date is within 2 days of the payment eligible date then flag the inf_n0
  if the 2 days falls within on a saturday or sunday (5,6) then the date diff should be 4 days
  */
  date(processed_date)
    <= 
      date_add(date(payment_eligible_at), interval case when extract(dayofweek from date(payment_eligible_at)) in (5, 6) 
      then 4 
      else 2 end day )then influencer_id else null end as paid_within_limit,
      DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), QUARTER), WEEK) + 1 week_of_qtr,
        DATE_DIFF(
  DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), DATE_TRUNC(CURRENT_DATE(), QUARTER),
  WEEK
) weeks_in_qtr
from c
where date(payment_eligible_at) >= date(last_thursday) and date(payment_eligible_at) <= current_thursday
  )
   select * from last_one_week

