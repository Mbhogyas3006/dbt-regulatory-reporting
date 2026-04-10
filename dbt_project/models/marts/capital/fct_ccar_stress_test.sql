-- models/marts/capital/fct_ccar_stress_test.sql
-- CCAR Stress Test Results — Baseline, Adverse, Severely Adverse scenarios
-- Federal Reserve requires this annually for large bank holding companies
-- Shows capital impact under stressed macro conditions

{{
    config(
        materialized = 'table',
        post_hook    = "grant select on {{ this }} to role REGULATORY_ANALYST_ROLE"
    )
}}

with stress as (
    select * from {{ ref('stg_stress_scenarios') }}
),

loans as (
    select
        loan_id,
        asset_class,
        sector,
        region,
        internal_rating,
        ead_estimate,
        reporting_date
    from {{ ref('stg_loan_portfolio') }}
),

-- Join stress scenarios with loan attributes
stress_enriched as (
    select
        s.scenario_id,
        s.loan_id,
        s.scenario_name,
        s.scenario_year,
        s.reporting_date,

        -- Loan attributes
        l.asset_class,
        l.sector,
        l.region,
        l.internal_rating,
        l.ead_estimate as baseline_ead,

        -- Stressed parameters
        s.stressed_pd,
        s.stressed_lgd,
        s.stressed_ead,
        s.expected_loss,
        s.stressed_rwa,
        s.capital_impact,

        -- Incremental stress vs baseline
        s.expected_loss - (l.ead_estimate * 0.02 * 0.40) as incremental_loss

    from stress s
    left join loans l on s.loan_id = l.loan_id
),

-- Aggregate by scenario for executive summary
scenario_summary as (
    select
        reporting_date,
        scenario_name,
        scenario_year,
        asset_class,
        sector,
        region,

        count(loan_id)              as loan_count,
        sum(baseline_ead)           as total_baseline_ead,
        sum(stressed_ead)           as total_stressed_ead,
        avg(stressed_pd)            as avg_stressed_pd,
        avg(stressed_lgd)           as avg_stressed_lgd,
        sum(expected_loss)          as total_expected_loss,
        sum(stressed_rwa)           as total_stressed_rwa,
        sum(capital_impact)         as total_capital_impact,
        sum(incremental_loss)       as total_incremental_loss,

        -- Stressed EAD growth vs baseline
        round(
            (sum(stressed_ead) - sum(baseline_ead))
            / nullif(sum(baseline_ead), 0) * 100
        , 2) as ead_stress_growth_pct,

        -- Loss rate
        round(
            sum(expected_loss) / nullif(sum(stressed_ead), 0) * 100
        , 4) as stressed_loss_rate_pct,

        current_timestamp() as _model_run_at

    from stress_enriched
    group by 1,2,3,4,5,6
),

-- Add scenario severity ranking
final as (
    select
        *,
        rank() over (
            partition by reporting_date, asset_class, sector, region, scenario_year
            order by total_expected_loss desc
        ) as severity_rank,

        case scenario_name
            when 'Baseline'          then 1
            when 'Adverse'           then 2
            when 'Severely Adverse'  then 3
        end as scenario_order,

        -- Flag scenarios exceeding capital buffers
        (total_capital_impact > 1_000_000_000) as exceeds_capital_buffer

    from scenario_summary
)

select * from final
order by reporting_date, scenario_order, asset_class
