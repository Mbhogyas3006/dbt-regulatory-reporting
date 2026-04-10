# dbt + Snowflake Regulatory Reporting Engine

Automated regulatory reporting pipeline built on dbt Core and Snowflake — Basel III risk-weighted asset calculations, capital adequacy monitoring, CCAR stress testing, and LCR/NSFR liquidity coverage reporting with Great Expectations data validation and GitHub Actions CI/CD.

![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6B6B?style=flat)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat&logo=githubactions&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

---

## Business Context

Quarterly regulatory reporting in banking involves assembling data from multiple source systems into standardized submission tables for regulators — the Federal Reserve, OCC, and FDIC. The process typically relies on manual SQL scripts with no version control, no automated quality checks, and no data lineage documentation. When auditors ask how a capital ratio was derived, the answer often requires days of manual investigation.

This pipeline automates the full reporting cycle. Source data is validated before it touches Snowflake. dbt transforms it through three governed layers. Every transformation is version-controlled, tested, and documented. Regulators can trace any number in the final submission back to the source loan tape through the dbt lineage graph.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       SOURCE DATA (6 tables)                     │
│  loan_portfolio · counterparties · rwa_components                │
│  capital_components · stress_scenarios · market_risk_positions   │
└──────────────────────────────────┬───────────────────────────────┘
                                   │
                                   ▼  Great Expectations
                                   │  40+ expectations across 5 suites
                                   │  Blocks load on any failure
                                   │
                          Snowflake RAW Schema
                                   │
                                   ▼  dbt run
┌──────────────────────────────────▼───────────────────────────────┐
│                         dbt Layers                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  STAGING (views)                                            │ │
│  │  Clean · Type cast · Filter · 5 models                      │ │
│  └─────────────────────────┬───────────────────────────────────┘ │
│                            │                                     │
│  ┌─────────────────────────▼───────────────────────────────────┐ │
│  │  INTERMEDIATE (table)                                       │ │
│  │  int_credit_risk_enriched                                   │ │
│  │  Expected Loss · LTV · Rating bucket · Watch list flag      │ │
│  └─────────────────────────┬───────────────────────────────────┘ │
│                            │                                     │
│  ┌─────────────────────────▼───────────────────────────────────┐ │
│  │  MARTS (tables — regulatory submission)                     │ │
│  │  credit_risk.fct_basel3_rwa_report                          │ │
│  │  capital.fct_capital_adequacy                               │ │
│  │  capital.fct_ccar_stress_test                               │ │
│  │  liquidity.fct_liquidity_coverage                           │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                                   │  dbt test (60+ schema tests)
                                   │  dbt docs generate
                                   ▼
                              Power BI Dashboard
                        Capital · RWA · CCAR · Liquidity
```

---

## Regulatory Reports Produced

| Report | Regulation | Update Frequency |
|---|---|---|
| Risk-weighted assets by asset class | Basel III Pillar 1 | Quarterly |
| CET1 / Tier 1 / Total Capital ratios | Basel III / CRR2 | Quarterly |
| CCAR stress test — 3 scenarios | Federal Reserve | Annual |
| Liquidity Coverage Ratio (LCR) | Basel III | Monthly |
| Net Stable Funding Ratio (NSFR) | Basel III | Quarterly |

---

## dbt Model Lineage

```
RAW (Snowflake tables)
└── STAGING (views — always fresh, zero storage cost)
      stg_loan_portfolio      stg_capital_components
      stg_counterparties      stg_stress_scenarios
      stg_rwa_components
            │
            └── INTERMEDIATE (table — computed once, reused by 3 marts)
                  int_credit_risk_enriched
                  Joins: loans + counterparties + RWA
                  Derives: Expected Loss, LTV, rating bucket, watch list flag
                        │
                        └── MARTS (tables — regulatory outputs)
                              credit_risk.fct_basel3_rwa_report
                              capital.fct_capital_adequacy
                              capital.fct_ccar_stress_test
                              liquidity.fct_liquidity_coverage
```

---

## Repository Structure

```
├── generate_data.py                       # Synthetic regulatory data — 4,945 rows
│
├── dbt_project/
│   ├── dbt_project.yml                   # Project config · regulatory threshold vars
│   ├── packages.yml                      # dbt-utils dependency
│   ├── macros/
│   │   └── regulatory_macros.sql         # Reusable EL, rating bucket, capital macros
│   └── models/
│       ├── staging/
│       │   ├── sources.yml               # Source definitions + 60+ schema tests
│       │   └── stg_*.sql                # 5 staging models
│       ├── intermediate/
│       │   └── int_credit_risk_enriched.sql
│       └── marts/
│           ├── credit/fct_basel3_rwa_report.sql
│           ├── capital/fct_capital_adequacy.sql
│           ├── capital/fct_ccar_stress_test.sql
│           └── liquidity/fct_liquidity_coverage.sql
│
├── great_expectations/
│   └── run_validations.py                # 40+ expectations across 5 validation suites
│
└── sql/
    ├── snowflake_setup.sql               # DDL, RBAC roles, raw tables
    └── analytics_queries.sql            # 8 business analytics queries
```

---

## Quick Start

```bash
pip install -r requirements.txt

# Generate synthetic source data
python generate_data.py

# Validate source data before loading to Snowflake
python great_expectations/run_validations.py

# Configure Snowflake connection
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"

# Run dbt pipeline
cd dbt_project
dbt deps              # install dbt-utils package
dbt run               # build all models
dbt test              # run 60+ schema tests
dbt docs generate     # generate lineage documentation
dbt docs serve        # open lineage DAG at localhost:8080
```

---

## CI/CD Pipeline

Every pull request triggers the full validation suite via GitHub Actions:

```
1. Generate synthetic data
2. dbt deps — install packages
3. dbt run --select staging
4. dbt test --select staging
5. dbt run — all models
6. dbt test — all models        ← PR blocked if any test fails
7. dbt docs generate
8. Great Expectations validation suite
```

---

## Synthetic Dataset

| Table | Rows | Description |
|---|---|---|
| loan_portfolio | 1,500 | Loans with PD, LGD, EAD, rating, asset class |
| counterparties | 200 | Counterparty master with KYC status and credit limits |
| rwa_components | 1,500 | Basel III RWA per loan — standardized approach |
| capital_components | 45 | Entity-level capital ratios by reporting period |
| stress_scenarios | 900 | CCAR scenarios — Baseline, Adverse, Severely Adverse |
| market_risk_positions | 800 | Trading book positions with VaR metrics |

All data is synthetically generated — no real regulatory or financial data.

---

## Key Engineering Decisions

| Decision | Rationale |
|---|---|
| Staging as views | Zero storage cost. Views always reflect the current state of raw tables — no stale materialization |
| Intermediate as table | Three large tables joined in one place and reused by multiple downstream marts. Computing once avoids redundant joins and ensures all marts see identical data |
| Marts as tables | Regulatory reports require consistent, predictable query time for Power BI DirectQuery and analyst ad-hoc queries |
| dbt vars for thresholds | CET1 minimum (4.5%), conservation buffer (2.5%), LCR floor (100%) defined once in `dbt_project.yml`. A threshold change updates every compliance flag in every model on the next `dbt run` |
| Great Expectations before dbt | Source validation runs before any data enters Snowflake. Invalid PD values, duplicate loan IDs, or missing fields stop the pipeline before transformation begins |
| Snowflake RBAC | `DBT_ROLE` for pipeline writes. `REGULATORY_ANALYST_ROLE` for read-only mart access. Mart tables grant access via dbt post-hook on every rebuild |
