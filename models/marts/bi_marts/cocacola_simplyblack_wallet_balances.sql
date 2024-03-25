With campaign_payments as
(SELECT 
    DATE_TRUNC(a.payment_date, MONTH) month,
    concat(FORMAT_DATETIME("%b", DATETIME(date(a.payment_date))),"-", extract(year from date(a.payment_date))) mon_yr,
    dense_rank () over (order by extract(year from a.payment_date) asc, extract(month from a.payment_date)asc ) mon_yr_rnk,
    a.campaign_name,
    a.company_name,
    a.client,
    a.brand,
    a.datasource,
    a.country,
    a.amount,
    a.amount_usd payable_amount_usd,
    case when a.payment_status = 'Successful' then a.amount_usd
    else null 
    end amount_usd
FROM `bi-staging-1-309112.wowzi_dbt_prod.cocacola_markets_payouts` a
    where a.campaign_name is not null
    and a.payment_date is not null
    and a.amount_usd > 0),

wallet_amount as 
(SELECT 
  'Simply Black' agency,
  sum(amount_usd) amount_usd
FROM `bi-staging-1-309112.wowzi_dbt_prod.simply_black_cashflow` ),

aggregate_campaign_payments as
(select 
    c.month,
    c.mon_yr,
    c.mon_yr_rnk,
    c.campaign_name,
    c.company_name,
    c.client,
    c.brand,
    c.datasource,
    c.country,
    sum(c.amount) amount,
    sum(c.payable_amount_usd) payable_amount_usd,
    sum(c.amount_usd) amount_usd,
    w.amount_usd wallet_amount,
    row_number() over(order by month,  country ) rnk
from campaign_payments c 
left join wallet_amount w 
on w.agency = w.agency
and w.amount_usd = w.Amount_USD
    group by c.month,c.mon_yr,c.mon_yr_rnk,c.campaign_name,c.company_name,c.client,c.brand,c.datasource,c.country, w.amount_usd
    order by month, country),

aggregate_campaign_payments_with_wallet_bal as
(select 
    c.month,
    c.mon_yr,
    c.mon_yr_rnk,
    c.campaign_name,
    c.company_name,
    c.client,
    c.brand,
    c.datasource,
    c.country,
    c.amount,
    c.payable_amount_usd,
    c.amount_usd,
    --c.wallet_amount,
    c.rnk,
    case when rnk = 1 then wallet_amount else 0 end as wallet_amt
from aggregate_campaign_payments c),

final_table as
(select 
    month,
    mon_yr,
    mon_yr_rnk,
    campaign_name,
    company_name,
    client,
    brand,
    datasource,
    country,
    amount,
    payable_amount_usd,
    amount_usd,
    rnk,
    wallet_amt,
    wallet_amt-amount_usd balance
from aggregate_campaign_payments_with_wallet_bal
    order by rnk)


select 
    month,
    mon_yr,
    mon_yr_rnk,
    campaign_name,
    company_name,
    client,
    brand,
    datasource,
    country,
    amount,
    rnk,
    payable_amount_usd,
    wallet_amt,
    amount_usd,
    balance,
    sum(balance) over(order by month, country desc 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wallet_balance
from final_table
    order by rnk