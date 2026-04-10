-- models/staging/stg_rwa_components.sql
with source as (
    select * from {{ source('raw', 'rwa_components') }}
),
renamed as (
    select
        loan_id,
        asset_class,
        internal_rating,
        cast(risk_weight          as decimal(10,4)) as risk_weight,
        cast(ead                  as decimal(20,2)) as ead,
        cast(credit_rwa           as decimal(20,2)) as credit_rwa,
        cast(operational_rwa      as decimal(20,2)) as operational_rwa,
        cast(market_rwa           as decimal(20,2)) as market_rwa,
        cast(total_rwa            as decimal(20,2)) as total_rwa,
        cast(capital_requirement  as decimal(20,2)) as capital_requirement,
        approach,
        cast(reporting_date as date) as reporting_date,
        current_timestamp() as _loaded_at
    from source
    where loan_id is not null
      and total_rwa >= 0
)
select * from renamed
