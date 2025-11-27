select 
*,
-- DATE_TRUNC(date(join_date), week(MONDAY)) =
--   DATE_SUB(date_trunc(current_date(), week(MONDAY)), INTERVAL 7 DAY)
--   THEN TRUE
extract(week(MONDAY) from date(week)) AS week_number,
case when 
    date_trunc(date(week), week(MONDAY)) =
    date_sub(date_trunc(current_date(), week(MONDAY)), interval 7 day)
    then TRUE
    else NULL
    end AS is_this_week,

case when 
    date_trunc(date(week), week(MONDAY)) =
    date_sub(date_trunc(current_date(), week(MONDAY)), interval 14 day)
    then TRUE
    else NULL
    end AS is_last_week
from
{{ source('staging', 'gsheets_safaricom_sentiments_influencers')}}