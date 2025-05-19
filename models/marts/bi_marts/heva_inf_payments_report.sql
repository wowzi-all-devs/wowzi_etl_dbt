with
fp as
(
  select 
  *,
    CASE 
    WHEN DATE(payment_eligible_at) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY) 
      THEN 'Next 5 Days' 
    
    WHEN DATE(payment_eligible_at) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY) 
      THEN 'Next 7 Days' 
      ELSE 'Beyond 7 Days' 
    END AS payable_days_flag,
    
  from bi-staging-1-309112.wowzi_airbyte.influencer_transfers
  where payment_eligible_at >= '2024-12-27'
  and lower(status) in ('completed', 'successful', 'manual', 'waiting_for_payment') 
  and influencer_id <> 126859
  order by date_created asc
 ),
inf as 
(
 select 
 inf.influencer_id,
 inf.company_id,
 initcap(inf.company_name) company_name,
 inf.campaign_id,
 inf.job_id,
 inf.task_id,
 inf.job_status,
 inf.gender,
 inf.age_groups,
 initcap(loc.income_category) income_category,
 inf.country,
 inf.job_offer_date,
 inf.task_creation_date,
 inf.no_of_tasks,
 inf.completed_tasks,
 initcap(inf.invitation_status) invitation_status,
 case when loc.location is null then 'Not Provided' else
 initcap(loc.location) end as location,
 inf.amount_lcy job_value,
 case when company_id IN (1191, 5957, 6121, 17398, 19907, 19773, 20248)
--   ('Safaricom', 'Mediacom', 'Uk-Kenya Tech Hub', 
--  'Equity Bank', 'Predator', 'Infinix', 'Kenya Tourism Board') 
 then 0 else 1 end as order_flg 
 FROM bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown inf
 LEFT JOIN bi-staging-1-309112.wowzi_dbt_prod.influencer_facts loc on inf.influencer_id = loc.influencer_id 
--  FROM {{ ref('influencer_job_breakdown') }} inf
--  LEFT JOIN {{ ref('influencer_facts') }} loc on inf.influencer_id = loc.influencer_id 
),
semi_final as
(
select 
 fp.id payment_id,
 fp.influencer_id,
 inf.company_name,
 inf.campaign_id,
 inf.job_id,
 fp.task_id,
 inf.job_status,
 inf.no_of_tasks,
 inf.completed_tasks,
 inf.gender,
 inf.age_groups,
 inf.income_category,
 inf.country,
 inf.location,
 CASE WHEN 
 fp.transfer_id = 0 then null else fp.transfer_id 
 end as transfer_id,
 fp.currency,
 inf.job_value,
 (fp.amount * 0.09) / 12 fast_pay_fee,
 fp.amount paid_amount,
 lower(fp.status) payment_status,
fp.provider,
 case when
 fp.provider <> 'MPESA_KE' THEN 'Cellulant'
 when 
 fp.provider is null THEN fp.provider
 ELSE 'MPESA' END as payment_channel,
 fp.reference,
 fp.payment_eligible_at payment_date,
 case when date(fp.payment_eligible_at) > current_date() then 'Future_payment' else 'Past_payment' end as payment_flag,
 extract(month from date(fp.payment_eligible_at)) mon,
 extract(year from date(fp.payment_eligible_at)) yr,
 concat(FORMAT_DATETIME("%b", DATETIME(date(fp.payment_eligible_at))),"-", extract(year from date(fp.payment_eligible_at))) mon_yr,
 dense_rank () over (order by extract(year from date(fp.payment_eligible_at)) asc, extract(month from date(fp.payment_eligible_at))asc) mon_yr_rnk,
fp.payable_days_flag,
 case 
  when 
  date(fp.payment_eligible_at) < current_date() 
  and lower(fp.status) = 'waiting_for_payment' 
  then 'old_unpaid' else 'Paid_or_upcoming'
  end as 
clean_payment_flag,
order_flg
 FROM fp 
 LEFT JOIN inf on fp.task_id = inf.task_id
 order by order_flg
),
final as 
(
  select 
*,
  SUM(paid_amount) OVER (
    PARTITION BY influencer_id, mon_yr
  ) AS period_total_paid,
  row_number() over (Partition by influencer_id, mon_yr_rnk order by payment_date) as paymnt_rnk,
    CASE 
    WHEN SUM(paid_amount) OVER (PARTITION BY influencer_id, mon_yr_rnk) < 20000 THEN '< 20K'
    WHEN SUM(paid_amount) OVER (PARTITION BY influencer_id, mon_yr_rnk) BETWEEN 20000 AND 49999 THEN '20K - 50K'
    WHEN SUM(paid_amount) OVER (PARTITION BY influencer_id, mon_yr_rnk) BETWEEN 50000 AND 99999 THEN '50K - 100K'
    WHEN SUM(paid_amount) OVER (PARTITION BY influencer_id, mon_yr_rnk) BETWEEN 100000 AND 249999 THEN '100K - 250K'
    ELSE '> 250K'
  END AS paid_bucket,

   CASE 
    WHEN SUM(paid_amount) OVER (PARTITION BY influencer_id) < 16500 THEN 'Below DFW'
    ELSE 'Above DFW'

  END AS DFW_Category
  from semi_final
)
select 
      payment_id, influencer_id, company_name, campaign_id, job_id,
       task_id, job_status, no_of_tasks, completed_tasks, gender,
       age_groups, income_category, country, location, transfer_id,
       currency, job_value,
       payment_status, provider, payment_channel, reference,
       payment_date, payment_flag, mon, yr, mon_yr, mon_yr_rnk,
       payable_days_flag, clean_payment_flag, order_flg,
       case when paymnt_rnk = 1 then period_total_paid else null end as period_total_paid, 
       fast_pay_fee, paid_amount,
       paid_bucket, DFW_Category
from final