select *, row_number() over (
   partition by Request_No_) as rn,
       extract(year from request_date_) mon,
    extract(year from request_date_) yr,
    concat(FORMAT_DATETIME("%b", DATETIME(date(request_date_))),"-", extract(year from date(request_date_))) mon_yr,
    dense_rank () over (order by extract(year from request_date_) asc, extract(month from request_date_)asc) mon_yr_rnk,
from
   {{ source('staging', 'gsheet__stg__HEVA_requsition_data_2')}}
where Request_No_ is not null