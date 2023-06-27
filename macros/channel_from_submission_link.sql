
{% macro submission_link_channel(submission_link)%}
case
    when submission_link like "%twitter%" or submission_link like "%t.co%" then 'Twitter'
    when submission_link like "%facebook%" then 'Facebook'
    when submission_link like "%instagram%" then "Instagram"
    when submission_link like "%titok%" then "Tiktok"
end
{% endmacro %}