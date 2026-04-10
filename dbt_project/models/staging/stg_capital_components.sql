-- models/staging/stg_capital_components.sql
with source as (
    select * from {{ source('raw', 'capital_components') }}
),
renamed as (
    select
        entity_id,
        reporting_period,
        cast(cet1_capital         as decimal(20,2)) as cet1_capital,
        cast(additional_tier1     as decimal(20,2)) as additional_tier1,
        cast(tier2_capital        as decimal(20,2)) as tier2_capital,
        cast(total_capital        as decimal(20,2)) as total_capital,
        cast(risk_weighted_assets as decimal(20,2)) as risk_weighted_assets,
        cast(cet1_ratio           as decimal(10,4)) as cet1_ratio,
        cast(tier1_ratio          as decimal(10,4)) as tier1_ratio,
        cast(total_capital_ratio  as decimal(10,4)) as total_capital_ratio,
        cast(leverage_ratio       as decimal(10,4)) as leverage_ratio,
        cast(lcr                  as decimal(10,4)) as lcr,
        cast(nsfr                 as decimal(10,4)) as nsfr,
        cast(reporting_date as date) as reporting_date,
        current_timestamp() as _loaded_at
    from source
    where entity_id is not null
      and cet1_capital > 0
)
select * from renamed
