{{ codegen.generate_base_model(
    source_name='staging',
    table_name='influencers',
    materialized='view'
) }}