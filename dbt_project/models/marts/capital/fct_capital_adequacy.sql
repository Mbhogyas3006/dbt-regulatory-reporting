-- models/marts/capital/fct_capital_adequacy.sql
-- Capital Adequacy Report — Basel III CET1, Tier 1, Total Capital Ratios
-- Tracks capital ratios against regulatory minimums and buffers
-- Required for CCAR and quarterly regulatory submissions

{{
    config(
        materialized = 'table',
        post_hook    = "grant select on {{ this }} to role REGULATORY_ANALYST_ROLE"
    )
}}

with capital as (
    select * from {{ ref('stg_capital_components') }}
),

-- Basel III regulatory thresholds (from dbt_project.yml vars)
thresholds as (
    select
        {{ var('cet1_minimum_ratio') }}            as cet1_minimum,
        {{ var('tier1_minimum_ratio') }}           as tier1_minimum,
        {{ var('total_capital_minimum_ratio') }}   as total_capital_minimum,
        {{ var('capital_conservation_buffer') }}   as conservation_buffer,
        {{ var('lcr_minimum') }}                   as lcr_minimum,
        {{ var('nsfr_minimum') }}                  as nsfr_minimum
),

capital_with_flags as (
    select
        c.entity_id,
        c.reporting_period,
        c.reporting_date,

        -- Capital components
        c.cet1_capital,
        c.additional_tier1,
        c.tier2_capital,
        c.total_capital,
        c.risk_weighted_assets,

        -- Regulatory ratios
        c.cet1_ratio,
        c.tier1_ratio,
        c.total_capital_ratio,
        c.leverage_ratio,
        c.lcr,
        c.nsfr,

        -- Thresholds
        t.cet1_minimum,
        t.tier1_minimum,
        t.total_capital_minimum,
        t.conservation_buffer,
        t.lcr_minimum,
        t.nsfr_minimum,

        -- ── Basel III compliance flags ─────────────────────────────────────

        -- CET1 compliance
        (c.cet1_ratio >= t.cet1_minimum)
            as meets_cet1_minimum,
        (c.cet1_ratio >= t.cet1_minimum + t.conservation_buffer)
            as meets_cet1_with_buffer,

        -- Tier 1 compliance
        (c.tier1_ratio >= t.tier1_minimum)
            as meets_tier1_minimum,

        -- Total capital compliance
        (c.total_capital_ratio >= t.total_capital_minimum)
            as meets_total_capital_minimum,

        -- Liquidity compliance
        (c.lcr  >= t.lcr_minimum)  as meets_lcr_requirement,
        (c.nsfr >= t.nsfr_minimum) as meets_nsfr_requirement,

        -- Overall compliance
        (
            c.cet1_ratio         >= t.cet1_minimum and
            c.tier1_ratio        >= t.tier1_minimum and
            c.total_capital_ratio >= t.total_capital_minimum and
            c.lcr                >= t.lcr_minimum and
            c.nsfr               >= t.nsfr_minimum
        ) as fully_compliant,

        -- ── Headroom calculations ──────────────────────────────────────────

        -- CET1 headroom above minimum
        round(c.cet1_ratio - t.cet1_minimum, 4)
            as cet1_headroom_pct,

        -- Distributable amount (above conservation buffer)
        round(c.cet1_ratio - (t.cet1_minimum + t.conservation_buffer), 4)
            as distributable_headroom_pct,

        -- Capital surplus / shortfall in absolute terms
        round(c.total_capital - (c.risk_weighted_assets * t.total_capital_minimum / 100), 2)
            as capital_surplus_shortfall,

        -- ── Period-over-period change (QoQ) ───────────────────────────────
        lag(c.cet1_ratio) over (
            partition by c.entity_id
            order by c.reporting_period
        ) as prior_period_cet1_ratio,

        round(
            c.cet1_ratio - lag(c.cet1_ratio) over (
                partition by c.entity_id order by c.reporting_period
            )
        , 4) as cet1_ratio_qoq_change,

        -- ── Risk classification ────────────────────────────────────────────
        case
            when c.cet1_ratio < t.cet1_minimum                              then 'Below Minimum — Regulatory Action Required'
            when c.cet1_ratio < t.cet1_minimum + t.conservation_buffer      then 'Within Buffer — Restricted Distributions'
            when c.cet1_ratio < t.cet1_minimum + t.conservation_buffer + 2  then 'Adequate — Monitor Closely'
            else 'Well Capitalised'
        end as capital_adequacy_status,

        current_timestamp() as _model_run_at

    from capital c
    cross join thresholds t
)

select * from capital_with_flags
order by entity_id, reporting_period
