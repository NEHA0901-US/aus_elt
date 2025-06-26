with source as (
    select * from {{ source('core_db', 'abr_entities') }}
),

validated as (
    select
        abn,
        entity_name,
        entity_type,
        entity_status,
        entity_address,
        entity_postcode,
        entity_state,
        entity_start_date,
        current_timestamp as ingested_at,

        -- Validations
        abn is not null and abn ~ '^[0-9]{11}$' as is_valid_abn,
        entity_name is not null and length(trim(entity_name)) > 0 as is_valid_entity_name,
        lower(trim(entity_type)) in (
            -- Example enum values (should be 77 known values)
            'State Government Company',
            'Local Government Private Company',
            'Local Government APRA Regulated Public Sector Scheme',
            'Commonwealth Government Non-Regulated Super Fund',
            'Commonwealth Government Entity',
            'Corporate Unit Trust',
            'Deceased Estate',
            'State Government Other Incorporated Entity',
            'Other Incorporated Entity',
            'State Government Trust',
            'Fixed Trust',
            'Commonwealth Government Statutory Authority',
            'State Government Other Unincorporated Entity',
            'Territory Government Discretionary Investment Trust',
            'Australian Private Company',
            'State Government Entity',
            'State Government Partnership',
            'State Government APRA Regulated Public Sector Scheme',
            'Territory Government Other Incorporated Entity',
            'Public Trading trust',
            'State Government Statutory Authority',
            'Hybrid Trust',
            'Corporate Collective Investment Vehicle (CCIV) Sub-Fund',
            'Local Government Entity',
            'Commonwealth Government Partnership',
            'APRA Regulated Public Offer Fund',
            'State Government Private Company',
            'State Government Public Company',
            'State Government Non-Regulated Super Fund',
            'Discretionary Services Management Trust',
            'Family Partnership',
            'Commonwealth Government Other Incorporated Entity',
            'Discretionary Investment Trust',
            'State Government Fixed Unit Trust',
            'Super Fund',
            'Local Government Company',
            'Other Partnership',
            'Commonwealth Government Discretionary Investment Trust',
            'Commonwealth Government Public Company',
            'Strata-title',
            'Commonwealth Government Private Company',
            'APRA Regulated Non-Public Offer Fund',
            'Fixed Unit Trust',
            'Limited Partnership',
            'Approved Deposit Fund',
            'Commonwealth Government Other Unincorporated Entity',
            'Commonwealth Government Discretionary Services Management Trust',
            'Other trust',
            'Cash Management Trust',
            'Local Government Discretionary Investment Trust',
            'Pooled Superannuation Trust',
            'Local Government Statutory Authority',
            'Pooled Development Fund',
            'Local Government Partnership',
            'Territory Government Entity',
            'Other Unincorporated Entity',
            'Discretionary Trading Trust',
            'Local Government Non-Regulated Super Fund',
            'Territory Government Statutory Authority',
            'Territory Government Other Unincorporated Entity',
            'Small APRA Fund',
            'Local Government Other Incorporated Entity',
            'ATO Regulated Self-Managed Superannuation Fund',
            'State Government Discretionary Services Management Trust',
            'State Government Fixed Trust',
            'Territory Government Non-Regulated Super Fund',
            'Individual/Sole Trader',
            'Non-Regulated Superannuation Fund',
            'Territory Government Fixed Trust',
            'Unlisted Public Unit Trust',
            'State Government Discretionary Investment Trust',
            'Listed Public Unit Trust',
            'First Home Saver Accounts Trust',
            'State Government Co-operative',
            'Local Government Other Unincorporated Entity',
            'Co-operative',
            'Australian Public Company'
        ) as is_known_entity_type,
        entity_status in ('ACT', 'CAN') as is_valid_status,
        length(cast(entity_postcode as text)) = 4 as is_valid_postcode,
        CAST(entity_start_date AS DATE) <= current_date and entity_start_date is not null as is_valid_start_date

    from source
),

flagged as (
    select *,
        case when not is_known_entity_type then true else false end as needs_verification
    from validated
),

filtered as (
    select *
    from flagged
    where
        is_valid_abn
        and is_valid_entity_name
        and is_valid_status
        and is_valid_postcode
        and is_valid_start_date
),

with_unique as (
    select distinct on (abn, lower(trim(entity_name))) *
    from filtered
    order by abn, lower(trim(entity_name)), ingested_at
)

select
    row_number() over () as auto_id,
    abn,
    trim(entity_name) as entity_name,
    lower(trim(entity_type)) as entity_type,
    entity_status,
    trim(entity_address) as entity_address,
    entity_postcode,
    trim(entity_state) as entity_state,
    entity_start_date,
    ingested_at,
    needs_verification
from with_unique