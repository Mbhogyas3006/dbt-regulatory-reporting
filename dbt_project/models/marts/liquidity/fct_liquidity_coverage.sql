-- models/marts/liquidity/fct_liquidity_coverage.sql
-- Liquidity Coverage Ratio (LCR) and Net Stable Funding Ratio (NSFR)
-- Basel III requires LCR >= 100% and NSFR >= 100%
-- This table feeds the treasury and liquidity risk team dashboard

{{
    config(
        materialized = 'table',
        post_hook    = "grant select on {{ this }} to role REGULATORY_ANALYST_ROLE"
    )
}}

with capital as (
    select * from {{ ref('stg_capital_components') }}
),

liquidity_metrics as (
    select
        entity_id,
        reporting_period,
        reporting_date,

        -- Liquidity ratios
        lcr,
        nsfr,
        leverage_ratio,

        -- Basel III minimums from project variables
        {{ var('lcr_minimum') }}  as lcr_minimum,
        {{ var('nsfr_minimum') }} as nsfr_minimum,

        -- Compliance flags
        (lcr  >= {{ var('lcr_minimum') }})  as meets_lcr,
        (nsfr >= {{ var('nsfr_minimum') }}) as meets_nsfr,
        (lcr  >= {{ var('lcr_minimum') }}
         and nsfr >= {{ var('nsfr_minimum') }}) as fully_liquid_compliant,

        -- Headroom
        round(lcr  - {{ var('lcr_minimum') }}, 2)  as lcr_headroom,
        round(nsfr - {{ var('nsfr_minimum') }}, 2)  as nsfr_headroom,

        -- Risk classification
        case
            when lcr < {{ var('lcr_minimum') }}
                then 'LCR Breach — Immediate Action'
            when lcr < {{ var('lcr_minimum') }} + 10
                then 'LCR Watch — Monitor Daily'
            else 'LCR Adequate'
        end as lcr_status,

        case
            when nsfr < {{ var('nsfr_minimum') }}
                then 'NSFR Breach — Immediate Action'
            when nsfr < {{ var('nsfr_minimum') }} + 5
                then 'NSFR Watch — Monitor Weekly'
            else 'NSFR Adequate'
        end as nsfr_status,

        -- Period over period change
        lag(lcr) over (
            partition by entity_id order by reporting_period
        ) as prior_lcr,

        round(lcr - lag(lcr) over (
            partition by entity_id order by reporting_period
        ), 2) as lcr_qoq_change,

        current_timestamp() as _model_run_at

    from capital
)

select * from liquidity_metrics
order by entity_id, reporting_period
