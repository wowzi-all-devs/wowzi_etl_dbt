select 
name,
date(join_date) as join_date,

case when 
 DATE_TRUNC(date(join_date), week(MONDAY)) =
  DATE_SUB(date_trunc(current_date(), week(MONDAY)), INTERVAL 7 DAY)
  THEN TRUE
    ELSE NULL
    END AS is_this_week,

case when 
 DATE_TRUNC(date(join_date), week(MONDAY)) =
  DATE_SUB(date_trunc(current_date(), week(MONDAY)), INTERVAL 14 DAY)
  THEN TRUE
    ELSE NULL
    END AS is_last_week,
from

{{source('staging', 'gsheets__safaricom_curators_influencers')}}