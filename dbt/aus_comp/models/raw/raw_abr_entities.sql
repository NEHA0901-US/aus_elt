-- dbt/models/raw/raw_abr_entities.sql

select
    abn,
    entity_name,
    entity_type,
    entity_status,
    entity_address,
    entity_postcode,
    entity_state,
    entity_start_date,
    ingested_at
from {{ source('core_db', 'abr_entities') }}