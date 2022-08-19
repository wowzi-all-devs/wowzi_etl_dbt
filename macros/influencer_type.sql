{% macro influencer_type(followers_count) %}
    CASE
        WHEN followers_count BETWEEN 0 AND 250 THEN "NONE"
        WHEN followers_count BETWEEN 250 AND 5000 THEN "NANO"
        WHEN followers_count BETWEEN 5000 AND 30000 THEN "MICRO"
        WHEN followers_count BETWEEN 30000 AND 100000 THEN "MACRO"
        WHEN followers_count BETWEEN 100000 AND 500000 THEN "MEGA"
        WHEN followers_count >500000 THEN "META"
        WHEN followers_count IS NULL THEN 'No Followers Reported'
    END
{% endmacro %}