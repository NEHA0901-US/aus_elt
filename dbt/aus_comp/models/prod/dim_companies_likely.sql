-- models/final/dim_companies_likely.sql

with abr as (
    select
        abn,
        entity_name,
        entity_type,
        entity_status,
        entity_address,
        entity_postcode,
        entity_state,
        entity_start_date,
        ingested_at as abr_ingested_at
    from {{ ref('stg_abr_entities') }}
),

web as (
    select
        url,
        company_name,
        industry,
        ingested_at as web_ingested_at
    from {{ ref('stg_commoncrawl_companies') }}
),

matched as (
    select
        a.abn,
        a.entity_name,
        a.entity_type,
        a.entity_status,
        a.entity_address,
        a.entity_postcode,
        a.entity_state,
        a.entity_start_date,
        a.abr_ingested_at,
        w.url,
        w.company_name,
        w.industry,
        w.web_ingested_at,
        levenshtein(lower(trim(a.entity_name)), lower(trim(w.company_name))) as name_distance
    from abr a
    cross join web w
    where levenshtein(lower(trim(a.entity_name)), lower(trim(w.company_name))) <= 2
)

select *
from matched
