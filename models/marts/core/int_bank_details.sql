-- Get updated bank details
select
    bd.influencer_id, bd.bank_name, bd.bank_account_number
FROM
    {{ source('staging', 'bank_details') }} bd
    inner join (
        select
            influencer_id,
            max(updated_at) max_date
        from
           {{ source('staging', 'bank_details') }} 
        group by
            1
    ) bd2 on bd.influencer_id = bd2.influencer_id
    and bd.updated_at = bd2.max_date