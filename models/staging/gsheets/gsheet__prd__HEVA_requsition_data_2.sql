select *, row_number() over (
   partition by Request_No_) as rn,
       extract(year from paid_out_date_) mon,
    extract(year from paid_out_date_) yr,
    concat(FORMAT_DATETIME("%b", DATETIME(date(paid_out_date_))),"-", extract(year from date(paid_out_date_))) mon_yr,
    dense_rank () over (order by extract(year from paid_out_date_) asc, extract(month from paid_out_date_)asc) mon_yr_rnk,
from
   {{ source('staging', 'gsheet__stg__HEVA_requsition_data_2')}}
where Request_No_ is not null