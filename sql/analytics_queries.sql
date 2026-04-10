-- ============================================================
-- Analytics Queries — dbt Regulatory Reporting Engine
-- Run against: Snowflake REGULATORY_DB schemas
-- Use for: interviews, Power BI, executive reporting
-- ============================================================

-- ── Q1. Basel III Capital Ratio Summary (Executive Dashboard) ─────────────────
-- Business question: Are all entities meeting Basel III minimums?
SELECT
    entity_id,
    reporting_period,
    ROUND(cet1_ratio, 2)                        AS cet1_ratio_pct,
    ROUND(tier1_ratio, 2)                        AS tier1_ratio_pct,
    ROUND(total_capital_ratio, 2)                AS total_capital_ratio_pct,
    capital_adequacy_status,
    meets_cet1_minimum,
    meets_cet1_with_buffer,
    ROUND(cet1_headroom_pct, 2)                  AS cet1_headroom_pct,
    ROUND(capital_surplus_shortfall / 1e9, 3)    AS capital_surplus_billions,
    fully_compliant
FROM REGULATORY_DB.CAPITAL.FCT_CAPITAL_ADEQUACY
ORDER BY reporting_period DESC, entity_id;


-- ── Q2. RWA by Asset Class (Basel III Schedule RC-R) ─────────────────────────
-- Business question: Where is our risk concentrated by asset class?
SELECT
    asset_class,
    rating_bucket,
    SUM(loan_count)                             AS loans,
    ROUND(SUM(total_ead) / 1e9, 3)              AS total_ead_billions,
    ROUND(SUM(total_rwa) / 1e9, 3)              AS total_rwa_billions,
    ROUND(AVG(avg_risk_weight) * 100, 1)        AS avg_risk_weight_pct,
    ROUND(SUM(total_expected_loss) / 1e6, 2)    AS expected_loss_millions,
    ROUND(AVG(exposure_weighted_pd) * 100, 3)   AS avg_pd_pct,
    SUM(defaulted_loan_count)                   AS defaulted_loans,
    ROUND(AVG(default_rate_pct), 2)             AS default_rate_pct
FROM REGULATORY_DB.CREDIT_RISK.FCT_BASEL3_RWA_REPORT
GROUP BY asset_class, rating_bucket
ORDER BY SUM(total_rwa) DESC;


-- ── Q3. CCAR Stress Test — Scenario Comparison ────────────────────────────────
-- Business question: How does capital impact differ across stress scenarios?
SELECT
    scenario_name,
    scenario_year,
    ROUND(SUM(total_baseline_ead) / 1e9, 2)     AS baseline_ead_billions,
    ROUND(SUM(total_stressed_ead) / 1e9, 2)     AS stressed_ead_billions,
    ROUND(SUM(total_expected_loss) / 1e9, 2)    AS expected_loss_billions,
    ROUND(SUM(total_capital_impact) / 1e9, 2)   AS capital_impact_billions,
    ROUND(AVG(stressed_loss_rate_pct), 3)        AS avg_loss_rate_pct,
    ROUND(AVG(ead_stress_growth_pct), 2)         AS avg_ead_growth_pct,
    SUM(CASE WHEN exceeds_capital_buffer THEN 1 ELSE 0 END) AS segments_exceeding_buffer
FROM REGULATORY_DB.CAPITAL.FCT_CCAR_STRESS_TEST
GROUP BY scenario_name, scenario_year
ORDER BY scenario_year, scenario_name;


-- ── Q4. Sector Concentration Risk ────────────────────────────────────────────
-- Business question: Are we over-concentrated in any sector?
SELECT
    sector,
    region,
    SUM(loan_count)                             AS loan_count,
    ROUND(SUM(total_ead) / 1e9, 3)              AS ead_billions,
    ROUND(SUM(total_ead) * 100.0
          / SUM(SUM(total_ead)) OVER(), 2)       AS ead_pct_of_total,
    ROUND(SUM(total_rwa) / 1e9, 3)              AS rwa_billions,
    ROUND(AVG(exposure_weighted_pd) * 100, 3)   AS avg_pd_pct,
    ROUND(AVG(el_ratio_pct), 4)                 AS avg_el_ratio_pct,
    SUM(watch_list_count)                       AS watch_list_loans
FROM REGULATORY_DB.CREDIT_RISK.FCT_BASEL3_RWA_REPORT
GROUP BY sector, region
ORDER BY ead_billions DESC;


-- ── Q5. Capital Ratio Trend (QoQ) ────────────────────────────────────────────
-- Business question: Is our capital position improving or deteriorating?
SELECT
    entity_id,
    reporting_period,
    ROUND(cet1_ratio, 2)                         AS cet1_ratio,
    ROUND(cet1_ratio_qoq_change, 4)              AS qoq_change,
    capital_adequacy_status,
    ROUND(lcr, 2)                                AS lcr,
    ROUND(nsfr, 2)                               AS nsfr,
    fully_compliant
FROM REGULATORY_DB.CAPITAL.FCT_CAPITAL_ADEQUACY
ORDER BY entity_id, reporting_period;


-- ── Q6. High Risk Loan Watch List ────────────────────────────────────────────
-- Business question: Which loans need immediate credit review?
SELECT
    l.loan_id,
    l.counterparty_id,
    c.counterparty_name,
    l.asset_class,
    l.sector,
    l.region,
    l.internal_rating,
    ROUND(l.ead_estimate / 1e6, 2)              AS ead_millions,
    ROUND(l.pd_estimate * 100, 2)               AS pd_pct,
    ROUND(l.lgd_estimate * 100, 2)              AS lgd_pct,
    ROUND(l.expected_loss / 1e6, 2)             AS expected_loss_millions,
    l.rating_bucket,
    l.ltv_ratio,
    l.is_defaulted,
    l.is_impaired,
    l.is_watch_list
FROM REGULATORY_DB.INTERMEDIATE.INT_CREDIT_RISK_ENRICHED l
LEFT JOIN REGULATORY_DB.STAGING.STG_COUNTERPARTIES c
    ON l.counterparty_id = c.counterparty_id
WHERE l.is_watch_list = TRUE
   OR l.is_defaulted  = TRUE
   OR l.is_impaired   = TRUE
ORDER BY l.ead_estimate DESC
LIMIT 100;


-- ── Q7. Liquidity Coverage Summary ───────────────────────────────────────────
-- Business question: Which entities are at risk of LCR/NSFR breaches?
SELECT
    entity_id,
    reporting_period,
    ROUND(lcr, 2)                               AS lcr,
    ROUND(nsfr, 2)                              AS nsfr,
    lcr_status,
    nsfr_status,
    ROUND(lcr_headroom, 2)                      AS lcr_headroom,
    ROUND(nsfr_headroom, 2)                     AS nsfr_headroom,
    fully_liquid_compliant
FROM REGULATORY_DB.LIQUIDITY.FCT_LIQUIDITY_COVERAGE
ORDER BY reporting_period DESC, lcr ASC;


-- ── Q8. dbt Model Lineage Check ───────────────────────────────────────────────
-- See all dbt models and their row counts (run after dbt run)
SELECT
    table_schema,
    table_name,
    row_count,
    bytes / 1024 / 1024                         AS size_mb,
    created
FROM REGULATORY_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('STAGING','INTERMEDIATE','CREDIT_RISK','CAPITAL','LIQUIDITY')
ORDER BY table_schema, table_name;
