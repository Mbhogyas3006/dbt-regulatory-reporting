# dbt + Snowflake Regulatory Reporting Engine

> **Enterprise-grade regulatory reporting pipeline** built on dbt Core and Snowflake — automating Basel III RWA calculations, capital adequacy monitoring, CCAR stress testing, and liquidity coverage reporting for a banking use case. Includes Great Expectations data validation and GitHub Actions CI/CD.

[![dbt](https://img.shields.io/badge/dbt_Core-1.7-orange)]()
[![Snowflake](https://img.shields.io/badge/Snowflake-Serving_Layer-cyan)]()
[![Great Expectations](https://img.shields.io/badge/Great_Expectations-Validation-blue)]()
[![CI](https://img.shields.io/badge/GitHub_Actions-CI%2FCD-green)]()
[![Python](https://img.shields.io/badge/Python-3.11-blue)]()

---

## Business Context

A bank's regulatory team manually assembles Basel III capital adequacy reports from 6 source systems every quarter. The process takes 3 weeks, involves two analysts, and has no data lineage — making it impossible to trace a capital ratio back to the source loan tape during audits.

Key problems:
- Manual SQL scripts with no version control
- No automated data quality checks — restatements are common
- Regulators cannot trace numbers back to source data
- CCAR stress test preparation takes an additional 2 weeks

**This pipeline solves it** — dbt transforms raw loan and capital data into Basel III–compliant regulatory tables in 4 hours, with full lineage, automated testing, and a CI/CD pipeline that blocks bad data from ever reaching the reporting layer.

---

## Architecture

```
Source CSV Files (6 tables)
  loan_portfolio · counterparties · rwa_components
  capital_components · stress_scenarios · market_risk_positions
          │
          ▼  Great Expectations (40+ validations)
          │  Blocks pipeline if source data fails quality checks
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Snowflake RAW Schema                      │
│  COPY INTO raw tables from CSV / source systems             │
└────────────────────────┬────────────────────────────────────┘
                         │  dbt run
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  dbt Transformation Layers                   │
│                                                             │
│  STAGING (views)      INTERMEDIATE (tables)   MARTS (tables)│
│  stg_loan_portfolio → int_credit_risk_    →  credit_risk/   │
│  stg_counterparties    enriched               fct_basel3_   │
│  stg_rwa_components                           rwa_report    │
│  stg_capital_comps                                          │
│  stg_stress_scenarios                        capital/       │
│                                               fct_capital_  │
│                                               adequacy      │
│                                               fct_ccar_     │
│                                               stress_test   │
│                                                             │
│                                              liquidity/     │
│                                               fct_liquidity_│
│                                               coverage      │
└─────────────────────────────────────────────────────────────┘
                         │  dbt test (60+ tests)
                         ▼
                  Power BI Dashboard
          Capital Ratios · RWA · CCAR · Liquidity
```

---

## Regulatory Reports Produced

| Report | Regulation | Update frequency |
|---|---|---|
| Basel III RWA by asset class | Basel III Pillar 1 | Quarterly |
| Capital Adequacy (CET1, Tier1, Total) | Basel III / CRR2 | Quarterly |
| CCAR Stress Test (3 scenarios) | Federal Reserve CCAR | Annual |
| Liquidity Coverage Ratio (LCR) | Basel III Pillar 1 | Monthly |
| Net Stable Funding Ratio (NSFR) | Basel III Pillar 1 | Quarterly |
| Credit Watch List | Internal risk policy | Weekly |

---

## dbt Model Lineage

```
RAW (Snowflake tables)
  └── STAGING (dbt views — clean, type-cast, filter)
        stg_loan_portfolio
        stg_counterparties
        stg_rwa_components
        stg_capital_components
        stg_stress_scenarios
              │
              └── INTERMEDIATE (dbt tables — join, enrich, derive)
                    int_credit_risk_enriched
                      (Expected Loss, LTV, rating bucket, watch list flag)
                              │
                              └── MARTS (dbt tables — regulatory reports)
                                    credit_risk.fct_basel3_rwa_report
                                    capital.fct_capital_adequacy
                                    capital.fct_ccar_stress_test
                                    liquidity.fct_liquidity_coverage
```

---

## Tech Stack

| Category | Technology |
|---|---|
| Transformation | dbt Core 1.7 |
| Data warehouse | Snowflake (RBAC, clustering, warehouse auto-suspend) |
| Data validation | Great Expectations (40+ expectations across 5 suites) |
| CI/CD | GitHub Actions (dbt run → dbt test → GE validate → dbt docs) |
| Language | SQL (dbt models), Python (data generator, GE runner) |
| dbt packages | dbt-utils 1.1.1 |

---

## Repository Structure

```
dbt-regulatory-reporting/
│
├── generate_data.py                    # Synthetic data generator (6 files, ~5K rows)
├── requirements.txt
├── .gitignore
│
├── data/                               # Generated CSV files (gitignored)
│
├── dbt_project/
│   ├── dbt_project.yml                 # Project config, vars, materializations
│   ├── packages.yml                    # dbt-utils dependency
│   ├── profiles.yml                    # Snowflake connection (use env vars)
│   ├── macros/
│   │   └── regulatory_macros.sql       # Reusable EL, rating bucket, capital macros
│   └── models/
│       ├── staging/
│       │   ├── sources.yml             # Source definitions + schema tests
│       │   ├── stg_loan_portfolio.sql
│       │   ├── stg_counterparties.sql
│       │   ├── stg_rwa_components.sql
│       │   ├── stg_capital_components.sql
│       │   └── stg_stress_scenarios.sql
│       ├── intermediate/
│       │   └── int_credit_risk_enriched.sql
│       └── marts/
│           ├── credit/
│           │   └── fct_basel3_rwa_report.sql
│           ├── capital/
│           │   ├── fct_capital_adequacy.sql
│           │   └── fct_ccar_stress_test.sql
│           └── liquidity/
│               └── fct_liquidity_coverage.sql
│
├── great_expectations/
│   └── run_validations.py              # 40+ data quality expectations
│
├── sql/
│   ├── snowflake_setup.sql             # DDL, RBAC, raw tables
│   └── analytics_queries.sql          # 8 business analytics queries
│
└── .github/
    └── workflows/
        └── dbt_ci.yml                  # CI/CD: dbt run + test + GE + docs
```

---

## Quick Start — Local Development

### Prerequisites
- Python 3.11+
- Snowflake account (free trial at snowflake.com)

### Step 1 — Install dependencies
```bash
git clone https://github.com/YOUR_USERNAME/dbt-regulatory-reporting
cd dbt-regulatory-reporting
pip install -r requirements.txt
```

### Step 2 — Generate synthetic data
```bash
python generate_data.py
# Creates 6 CSV files in data/ folder — 4,945 rows total
# ALL DATA IS SYNTHETIC — no real bank data
```

### Step 3 — Run Great Expectations validation
```bash
python great_expectations/run_validations.py
# Runs 40+ expectations on source CSV files
# Must PASS before loading to Snowflake
```

### Step 4 — Set up Snowflake
```bash
# Run sql/snowflake_setup.sql in Snowflake worksheet
# Creates database, schemas, RBAC roles, raw tables
# Then COPY INTO raw tables from your CSV files
```

### Step 5 — Configure dbt profile
```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
```

### Step 6 — Run dbt
```bash
cd dbt_project
dbt deps              # install dbt-utils
dbt debug             # test connection
dbt run               # build all models
dbt test              # run 60+ schema tests
dbt docs generate     # generate lineage docs
dbt docs serve        # open lineage DAG in browser
```

---

## Key Engineering Decisions

| Decision | Choice | Reason |
|---|---|---|
| Staging materialization | Views | No storage cost, always fresh from raw |
| Intermediate materialization | Tables | Expensive joins computed once, reused by multiple marts |
| Mart materialization | Tables | Regulatory reports need consistent query performance |
| dbt vars for thresholds | `dbt_project.yml` | Change Basel III ratios in one place, all models update |
| GE before dbt | Pre-load validation | Bad source data never reaches Snowflake raw tables |
| RBAC on mart tables | post-hook GRANT | Analysts get read-only access — engineers write |
| Schema per domain | credit_risk/capital/liquidity | Matches regulatory org structure, clean separation |

---

## CI/CD Pipeline

Every push to `main` or `develop` triggers:

```
1. Generate synthetic data
2. Install dbt packages (dbt deps)
3. Run staging models (dbt run --select staging)
4. Test staging (dbt test --select staging)
5. Run all models (dbt run)
6. Run all tests (dbt test)  ← blocks merge if any test fails
7. Generate dbt docs
8. Run Great Expectations validation suite
```

If any step fails → PR is blocked from merging into main.

---

## Interview Talking Points

**On dbt layers:** "Staging is views — no storage cost, always fresh from raw. Intermediate is a table because I join 3 large tables there and 2 marts read from it — computing it once saves cost. Marts are tables because regulatory reports need predictable query time and the data changes only quarterly."

**On dbt vars for Basel thresholds:** "The CET1 minimum ratio is defined once in dbt_project.yml. Every model that references it uses `{{ var('cet1_minimum_ratio') }}`. When Basel IV changes the minimum from 4.5% to 5.0%, I change one line and run `dbt run` — every compliance flag across every model updates automatically."

**On GE before dbt:** "The Great Expectations suite runs before data lands in Snowflake. If any source file has PD values outside 0-1, or LGD values above 1, or duplicate loan IDs — the pipeline stops and raises an alert. Bad data never reaches the regulatory tables. That's the difference between a data quality framework and data quality theater."

**On RBAC:** "Snowflake RBAC is set up so the dbt service account has write access to all schemas. Analysts only get SELECT on the three mart schemas — credit_risk, capital, liquidity. They can never touch staging or intermediate. The GRANT is in the dbt model's post-hook so it runs automatically every time the table is rebuilt."

**On lineage:** "dbt docs generate creates a full interactive lineage DAG. An auditor can click on fct_capital_adequacy and trace every column back through intermediate, staging, all the way to the raw Snowflake table and then to the source CSV. That is what replaces the 3-week manual audit prep — the lineage is always current because dbt generates it from the actual code."

---

## Resume Bullets

> "Built a dbt + Snowflake regulatory reporting pipeline automating Basel III RWA calculations, capital adequacy monitoring, and CCAR stress testing across 5 source datasets — reducing quarterly report preparation from 3 weeks to 4 hours with full automated data lineage"

> "Implemented Great Expectations data validation suite with 40+ expectations blocking pipeline execution on source data failures — ensuring zero defective data reaches regulatory submission tables"

> "Designed CI/CD pipeline using GitHub Actions running dbt build, dbt test, and Great Expectations validation on every pull request — enforcing code quality and data quality gates before any model reaches production Snowflake"

---

## Author

**Bhogya Swetha Malladi** · Data Engineer · New York, NY
*dbt · Snowflake · Great Expectations · GitHub Actions · Python · SQL · Financial Services*
