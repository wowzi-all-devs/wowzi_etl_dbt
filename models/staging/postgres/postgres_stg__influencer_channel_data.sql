SELECT
    id,
    channel,
    influencer_id,
    cast(followers_count as INT) as followers_count ,
    CASE
        WHEN STARTS_WITH(username, 'https://www.instagram.com/') THEN 
            SUBSTR(
                username,
                LENGTH('https://www.instagram.com/') + 1,
                STRPOS(SUBSTR(username, LENGTH('https://www.instagram.com/') + 1), '/') - 1
            )
        WHEN STARTS_WITH(username, 'https://instagram.com/') THEN 
            SUBSTR(
                username,
                LENGTH('https://instagram.com/') + 1,
                CASE 
                    WHEN STRPOS(username, '?igshid=') > 0 THEN
                        STRPOS(username, '?igshid=') - LENGTH('https://instagram.com/') - 1
                    ELSE
                        LENGTH(username) - LENGTH('https://instagram.com/')
                END
            )
        ELSE
            username
    END AS username,
    status,
    status_backofficer_id,
    status_comment,
    status_update_time
FROM {{ source('staging', 'influencer_channel_data') }}