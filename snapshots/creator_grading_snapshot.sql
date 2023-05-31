{% snapshot creator_grading_snapshot %}

{{
    config(
      target_schema='wowzi_snapshots',
      unique_key='influencer_id',
      strategy='check',
      check_cols=['wowzi_rank', 'creator_segment'],
    )
}}

SELECT  
    influencer_id,
    wowzi_rank,
    creator_segment
FROM {{ ref('influencer_social_scoring') }}

{% endsnapshot %}