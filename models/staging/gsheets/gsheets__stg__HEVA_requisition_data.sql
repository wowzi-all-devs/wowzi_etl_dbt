select  
    Summary_of_Requisitions_ requisition_count,
    Request_Date_ request_date,
    Amount_ requested_amount,
    Paid_Out_Date_ payout_date,
    Replenish_Date_ replenish_date,
    Replenished_Amount_ replenished_amount,
    extract(year from Paid_Out_Date_ ) mon,
    extract(year from Paid_Out_Date_ ) yr,
    dense_rank () over (order by extract(year from Paid_Out_Date_ ) asc, extract(month from Paid_Out_Date_ )asc) mon_yr_rnk,
    Balance_ balance
 FROM 
    {{ source('staging', 'gsheet__stg__HEVA_requisition_data')}}