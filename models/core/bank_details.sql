select 
    id,
    influencer_id,
    recipient_id,
    bank_account_number,
    bank_code,
    fullname,
    bank_name,
    destination_branch_code
FROM {{ source('core', 'bank_details') }}