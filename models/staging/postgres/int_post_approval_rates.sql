select *,
    round(
        (first_verification_approvals / total_campaigns),
        2
    ) as first_verification_approval_rate,
    round(
        (second_verification_approvals / total_campaigns),
        2
    ) as second_verification_approval_rate,
    round(
        (third_verification_approvals / total_campaigns),
        2
    ) as third_verification_approval_rate,
    from (
        SELECT influencer_id,
            count(distinct campaign_id) as total_campaigns,
            sum(
                if (first_verification_status = "APPROVED", 1, 0)
            ) as first_verification_approvals,
            sum(
                if (second_verification_status = "APPROVED", 1, 0)
            ) as second_verification_approvals,
            sum(
                if (third_verification_status = "APPROVED", 1, 0)
            ) as third_verification_approvals,
            sum(payment_amount_netto) as total_net_payment,
            -- FROM `bi-staging-1-309112.{}.{}`
            FROM {{ ref('postgres_stg__influencer_tasks') }}
        group by influencer_id
        order by influencer_id desc
    )