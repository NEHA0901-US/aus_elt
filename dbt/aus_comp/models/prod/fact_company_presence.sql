-- dbt/models/final/fact_company_presence.sql

with matched as (
    select * from {{ ref('dim_companies') }}
)

select
    abn,
    url,
    entity_name,
    industry,
    abr_ingested_at,
    web_ingested_at
from matched
