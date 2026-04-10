-- models/staging/stg_counterparties.sql
with source as (
    select * from {{ source('raw', 'counterparties') }}
),
renamed as (
    select
        counterparty_id,
        counterparty_name,
        counterparty_type,
        sector,
        country,
        region,
        external_rating,
        cast(is_pep        as boolean) as is_pep,
        cast(is_sanctioned as boolean) as is_sanctioned,
        kyc_status,
        cast(onboarding_date  as date) as onboarding_date,
        cast(last_review_date as date) as last_review_date,
        cast(credit_limit as decimal(20,2)) as credit_limit,
        cast(reporting_date as date) as reporting_date,
        current_timestamp() as _loaded_at
    from source
    where counterparty_id is not null
      and is_sanctioned = false
)
select * from renamed
