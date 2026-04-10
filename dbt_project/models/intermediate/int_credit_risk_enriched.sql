-- models/intermediate/int_credit_risk_enriched.sql
-- Joins loan portfolio with counterparty data and RWA components
-- Adds derived risk metrics used across multiple downstream marts
-- This is the core intermediate table — all credit risk marts read from here

with loans as (
    select * from {{ ref('stg_loan_portfolio') }}
),

counterparties as (
    select * from {{ ref('stg_counterparties') }}
),

rwa as (
    select * from {{ ref('stg_rwa_components') }}
),

enriched as (
    select
        -- Loan identifiers
        l.loan_id,
        l.counterparty_id,

        -- Counterparty attributes
        c.counterparty_name,
        c.counterparty_type,
        c.kyc_status,
        c.is_pep,
        c.external_rating,

        -- Loan classification
        l.asset_class,
        l.product_type,
        l.sector,
        l.region,
        l.country,
        l.currency,
        l.collateral_type,

        -- Exposures
        l.exposure_amount,
        l.outstanding_balance,
        l.ead_estimate,
        l.collateral_value,

        -- Risk parameters
        l.internal_rating,
        l.pd_estimate,
        l.lgd_estimate,

        -- RWA components
        r.risk_weight,
        r.credit_rwa,
        r.operational_rwa,
        r.market_rwa,
        r.total_rwa,
        r.capital_requirement,
        r.approach,

        -- Dates
        l.origination_date,
        l.maturity_date,
        l.reporting_date,

        -- Status
        l.is_defaulted,
        l.is_impaired,

        -- ── Derived risk metrics ───────────────────────────────────────────

        -- Expected Loss = PD × LGD × EAD
        round(l.pd_estimate * l.lgd_estimate * l.ead_estimate, 2)
            as expected_loss,

        -- Unexpected Loss proxy
        round(l.ead_estimate * l.lgd_estimate *
              sqrt(l.pd_estimate * (1 - l.pd_estimate)), 2)
            as unexpected_loss_proxy,

        -- Loan-to-Value ratio (where collateral exists)
        case
            when l.collateral_value > 0
            then round(l.outstanding_balance / l.collateral_value, 4)
            else null
        end as ltv_ratio,

        -- Collateral coverage ratio
        case
            when l.ead_estimate > 0
            then round(l.collateral_value / l.ead_estimate, 4)
            else 0
        end as collateral_coverage_ratio,

        -- Risk rating bucket for reporting
        case
            when l.internal_rating in ('AAA','AA+','AA','AA-') then 'Investment Grade — High'
            when l.internal_rating in ('A+','A','A-')          then 'Investment Grade — Upper Mid'
            when l.internal_rating in ('BBB+','BBB','BBB-')    then 'Investment Grade — Lower Mid'
            when l.internal_rating in ('BB+','BB','BB-')       then 'Sub-Investment Grade — High'
            when l.internal_rating in ('B','CCC')              then 'Sub-Investment Grade — Low'
            else 'Unrated'
        end as rating_bucket,

        -- PD bucket for Basel reporting
        case
            when l.pd_estimate < 0.01  then '<1%'
            when l.pd_estimate < 0.05  then '1-5%'
            when l.pd_estimate < 0.10  then '5-10%'
            when l.pd_estimate < 0.20  then '10-20%'
            else '>20%'
        end as pd_bucket,

        -- Maturity bucket
        case
            when datediff('year', l.reporting_date, l.maturity_date) <= 1  then '0-1Y'
            when datediff('year', l.reporting_date, l.maturity_date) <= 3  then '1-3Y'
            when datediff('year', l.reporting_date, l.maturity_date) <= 5  then '3-5Y'
            when datediff('year', l.reporting_date, l.maturity_date) <= 10 then '5-10Y'
            else '10Y+'
        end as maturity_bucket,

        -- Watch list flag
        (l.pd_estimate > 0.10 or l.is_impaired or l.ltv_ratio > 1.2)
            as is_watch_list

    from loans l
    left join counterparties c
        on l.counterparty_id = c.counterparty_id
    left join rwa r
        on l.loan_id = r.loan_id
)

select * from enriched
