WITH cohort_items AS
(SELECT 
  Client,
  min(Month) as cohort_month
FROM {{ source('staging', 'mom_client_revenue') }} 
  group by Client),

client_activities as
(SELECT 
  a.Client,
  a.Month,
  date_diff(a.Month, b.cohort_month, month) month_number
FROM {{ source('staging', 'mom_client_revenue') }} a 
left join cohort_items b on a.Client = b.Client),

cohort_size as
(select 
    a.cohort_month,
    count(a.Client) num_clients
from cohort_items a
    group by cohort_month
    order by cohort_month),

retention_table as
(select
    b.cohort_month,
    a.month_number,
    count(a.Client) num_clients
from client_activities a 
left join cohort_items b on a.Client =b.Client
    group by b.cohort_month, a.month_number)

select 
    a.cohort_month,
    concat( FORMAT_DATETIME("%b", a.cohort_month ),"-", extract(year from a.cohort_month) ) cohort_mon_yr,
    FORMAT_DATETIME("%b", DATETIME(date(a.cohort_month))) cohort_mon,
    extract(year from a.cohort_month) cohort_yr,
    dense_rank () over (order by extract(year from a.cohort_month) asc, extract(month from a.cohort_month)asc ) cohort_yr_rnk,
    b.num_clients total_clients,
    a.month_number,
    cast(a.num_clients as numeric)/b.num_clients retention_rate
from retention_table a
left join cohort_size b on a.cohort_month = b.cohort_month
    where a.cohort_month is not null
    order by a.cohort_month, a.month_number