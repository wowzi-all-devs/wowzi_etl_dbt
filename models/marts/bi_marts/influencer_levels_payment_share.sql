WITH job_breakdown as
(select
    extract(month from job_offer_date) mon_no,
    FORMAT_DATETIME("%b", DATETIME(date(job_offer_date))) mon,
    extract(year from job_offer_date) yr,
    mon_yr,
    influencer_level,
    amount_usd
 FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown`),

table1 as
(select
 mon_no,mon,yr,mon_yr,influencer_level, sum(amount_usd) total_payout
 from job_breakdown
 group by mon_no,mon,yr,mon_yr,influencer_level, amount_usd),

table2 as (
 select
    mon_no,mon,yr,mon_yr,influencer_level, sum(total_payout) tot
 from table1
 group by mon_no,mon,yr,mon_yr,influencer_level)

 select
 mon_no,mon,yr,mon_yr,influencer_level, tot, sum(tot) over(partition by mon_yr) payout_window, (tot / sum(tot) over(partition by mon_yr)) share_of_total, dense_rank () over (order by yr asc, mon_no asc) mon_yr_rnk
 from table2
 group by mon_no,mon,yr,mon_yr,influencer_level, tot