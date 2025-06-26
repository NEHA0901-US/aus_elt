-- dbt/models/raw/raw_commoncrawl.sql

select
    url,
    company_name,
    industry,
    ingested_at
from {{ source('core_db', 'commoncrawl_companies') }}