-- models/marts/credit/fct_basel3_rwa_report.sql
-- Basel III Risk-Weighted Asset report by asset class and rating
-- This is the primary regulatory submission table for credit risk capital
-- Regulators receive this quarterly — accuracy is legally required

{{
    config(
        materialized = 'table',
        post_hook    = "grant select on {{ this }} to role REGULATORY_ANALYST_ROLE"
    )
}}

with credit_risk as (
    select * from {{ ref('int_credit_risk_enriched') }}
),

-- Aggregate by asset class and rating bucket for Basel III Schedule
rwa_by_class as (
    select
        reporting_date,
        asset_class,
        rating_bucket,
        internal_rating,
        approach,
        currency,
        region,

        -- Exposure metrics
        count(loan_id)                          as loan_count,
        sum(exposure_amount)                    as total_exposure,
        sum(outstanding_balance)                as total_outstanding,
        sum(ead_estimate)                       as total_ead,
        sum(collateral_value)                   as total_collateral,

        -- RWA components
        sum(credit_rwa)                         as total_credit_rwa,
        sum(operational_rwa)                    as total_operational_rwa,
        sum(market_rwa)                         as total_market_rwa,
        sum(total_rwa)                          as total_rwa,

        -- Capital requirement (8% of RWA per Basel III minimum)
        sum(capital_requirement)                as total_capital_requirement,

        -- Risk parameters (exposure-weighted averages)
        round(
            sum(pd_estimate * ead_estimate) / nullif(sum(ead_estimate), 0)
        , 6)                                    as exposure_weighted_pd,
        round(
            sum(lgd_estimate * ead_estimate) / nullif(sum(ead_estimate), 0)
        , 6)                                    as exposure_weighted_lgd,

        -- Expected loss
        sum(expected_loss)                      as total_expected_loss,

        -- Default metrics
        sum(case when is_defaulted then 1 else 0 end)  as defaulted_loan_count,
        sum(case when is_defaulted then ead_estimate else 0 end) as defaulted_ead,
        sum(case when is_watch_list then 1 else 0 end) as watch_list_count,

        -- Average risk weight
        round(
            sum(total_rwa) / nullif(sum(ead_estimate), 0)
        , 4)                                    as avg_risk_weight,

        current_timestamp()                     as _model_run_at

    from credit_risk
    group by 1,2,3,4,5,6,7
),

-- Add regulatory ratios and flags
final as (
    select
        *,

        -- EL ratio (Expected Loss / Total EAD)
        round(
            total_expected_loss / nullif(total_ead, 0) * 100
        , 4)                                    as el_ratio_pct,

        -- Default rate
        round(
            defaulted_loan_count * 100.0 / nullif(loan_count, 0)
        , 4)                                    as default_rate_pct,

        -- Collateral coverage
        round(
            total_collateral / nullif(total_ead, 0)
        , 4)                                    as collateral_coverage_ratio,

        -- Capital adequacy flag
        (total_capital_requirement / nullif(total_rwa, 0) >= 0.08)
                                                as meets_minimum_capital,

        -- High risk flag for regulatory attention
        (exposure_weighted_pd > 0.10 or avg_risk_weight > 1.0)
                                                as high_risk_segment

    from rwa_by_class
)

select * from final
order by reporting_date, total_rwa desc
