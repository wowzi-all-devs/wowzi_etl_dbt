WITH isc AS
(
       SELECT influencer_id,
              subcategory_id
    --    FROM   {{ ref ('postgres_stg__influencer_subcategories') }}
       from `bi-staging-1-309112.wowzi_airbyte.influencer_subcategories` 
       WHERE  subcategory_type IN ('INTERESTING','POSTING') 
), sc AS
(
       SELECT id,
              parent_category_id,
              initcap(en_label) en_label
    --    FROM   {{ ref ('postgres_stg__subcategories') }} 
          from `bi-staging-1-309112.wowzi_airbyte.subcategories`
), ---sub_category to inf sub_category on sc.id = isc.subcategory_id
cat AS
(
       SELECT id,
              en_label category
    --    FROM   {{ ref ('postgres_stg__categories') }} 
        from bi-staging-1-309112.wowzi_airbyte.categories
), ---sub_category to parent_category_id on sc.id = c.id
inf_sub_cate AS
(
          SELECT    influencer_id,
                    parent_category_id,
                    en_label
                    -- en_label
          FROM      isc
          LEFT JOIN sc
          ON        isc.subcategory_id = sc.id 
), semi AS
(
          SELECT    influencer_id,
                    parent_category_id,
                    en_label,
                    category,
                    row_number () OVER (partition BY influencer_id, parent_category_id) rnk
          FROM      inf_sub_cate
          LEFT JOIN cat
          ON        inf_sub_cate.parent_category_id = cat.id 
)
SELECT   influencer_id,
         array_to_string(array_agg(DISTINCT category ORDER BY category), ', ') AS categories
FROM     semi
WHERE    rnk = 1
-- AND      influencer_id IN (12165,134199)
GROUP BY influencer_id