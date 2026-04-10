-- models/staging/stg_loan_portfolio.sql
-- Staging model: raw loan portfolio → clean, typed, renamed
-- Source: Snowflake raw.loan_portfolio (loaded from CSV)

with source as (
    select * from {{ source('raw', 'loan_portfolio') }}
),

renamed as (
    select
        -- Keys
        loan_id,
        counterparty_id,

        -- Classification
        asset_class,
        product_type,
        sector,
        region,
        country,
        currency,

        -- Exposures (cast to ensure correct types)
        cast(exposure_amount     as decimal(20,2)) as exposure_amount,
        cast(outstanding_balance as decimal(20,2)) as outstanding_balance,
        cast(committed_amount    as decimal(20,2)) as committed_amount,
        cast(ead_estimate        as decimal(20,2)) as ead_estimate,
        cast(collateral_value    as decimal(20,2)) as collateral_value,

        -- Risk parameters
        internal_rating,
        cast(pd_estimate  as decimal(10,6)) as pd_estimate,
        cast(lgd_estimate as decimal(10,6)) as lgd_estimate,
        collateral_type,

        -- Dates
        cast(origination_date as date) as origination_date,
        cast(maturity_date    as date) as maturity_date,
        cast(reporting_date   as date) as reporting_date,

        -- Status flags
        cast(is_defaulted as boolean) as is_defaulted,
        cast(is_impaired  as boolean) as is_impaired,

        -- Metadata
        source_system,
        current_timestamp() as _loaded_at

    from source
    where loan_id is not null
      and exposure_amount > 0
      and reporting_date is not null
)

select * from renamed
