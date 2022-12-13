select 
    id as backofficer_id,
    concat(first_name, ' ', last_name) as backofficer_name,
    email,
    role
FROM {{ source('staging', 'backofficers') }}