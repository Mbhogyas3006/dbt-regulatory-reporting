-- models/staging/stg_stress_scenarios.sql
with source as (
    select * from {{ source('raw', 'stress_scenarios') }}
),
renamed as (
    select
        scenario_id,
        loan_id,
        scenario_name,
        scenario_year,
        cast(stressed_pd       as decimal(10,6)) as stressed_pd,
        cast(stressed_lgd      as decimal(10,6)) as stressed_lgd,
        cast(stressed_ead      as decimal(20,2)) as stressed_ead,
        cast(expected_loss     as decimal(20,2)) as expected_loss,
        cast(stressed_rwa      as decimal(20,2)) as stressed_rwa,
        cast(capital_impact    as decimal(20,2)) as capital_impact,
        cast(reporting_date as date) as reporting_date,
        current_timestamp() as _loaded_at
    from source
    where scenario_id is not null
      and scenario_name in ('Baseline', 'Adverse', 'Severely Adverse')
      and stressed_pd between 0 and 1
      and stressed_lgd between 0 and 1
)
select * from renamed
