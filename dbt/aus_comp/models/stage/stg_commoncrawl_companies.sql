with source as (
    select * from {{ source('core_db', 'commoncrawl_companies') }}
),

validated as (
    select
        url,
        company_name,
        industry,
        current_timestamp as ingested_at,

        -- URL validation flags
        url ~* '^https?://.*\..+'  as is_valid_url,
        length(split_part(split_part(url, '://', 2), '/', 1)) <= 253 as is_url_length_ok,
        regexp_replace(split_part(split_part(url, '://', 2), '/', 1), '[^a-zA-Z0-9.-]', '') = split_part(split_part(url, '://', 2), '/', 1) as has_allowed_chars,

        -- company_name validation
        company_name is not null and length(trim(company_name)) > 0 as is_valid_company,

        -- industry validation (if not null)
        case when industry is not null then regexp_replace(industry, '[^a-zA-Z0-9 -]', '') = industry else true end as is_valid_industry

    from source
),

filtered as (
    select *
    from validated
    where
        is_valid_url
        and is_url_length_ok
        and has_allowed_chars
        and is_valid_company
        and is_valid_industry
),

with_unique as (
    select distinct on (lower(trim(url)), lower(trim(company_name))) *
    from filtered
    order by lower(trim(url)), lower(trim(company_name)), ingested_at
)

select
    row_number() over () as auto_id,
    url,
    trim(company_name) as company_name,
    trim(industry) as industry,
    ingested_at,
    lower(trim(url)) as url_normalized,
    lower(trim(company_name)) as company_name_normalized
from with_unique
