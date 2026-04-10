# PROJECT COVER SHEET
## dbt + Snowflake Regulatory Reporting Engine

---

**Engineer:** Bhogya Swetha Malladi
**GitHub repo:** `dbt-regulatory-reporting`
**Domain:** Banking / Regulatory Reporting / Risk
**Status:** Portfolio Project — synthetic data, production-grade patterns

---

## Business Problem

A bank's regulatory team manually assembles Basel III reports from 6 source
systems every quarter. It takes 3 weeks, produces restatements, and has zero
data lineage — auditors cannot trace a capital ratio back to the source loan tape.
CCAR stress test prep takes an additional 2 weeks on top.

**Outcome:** Fully automated regulatory pipeline — 4 hours from raw data to
compliant submission tables, with full lineage, 60+ automated tests, and
CI/CD that blocks bad data from ever reaching the reporting layer.

---

## Architecture Summary

```
CSV sources → Great Expectations → Snowflake RAW → dbt (Staging → Intermediate → Marts) → Power BI
                 (40+ checks)                        (views)       (tables)       (tables)
```

---

## Key Technical Features

| Feature | Implementation |
|---|---|
| dbt layer design | Staging (views) → Intermediate (tables) → Marts (tables) |
| Basel III compliance | RWA by asset class, CET1/Tier1/Total ratios, LCR, NSFR |
| CCAR stress testing | 3 scenarios (Baseline, Adverse, Severely Adverse) |
| dbt vars | Regulatory thresholds (CET1 4.5%, buffer 2.5%) in one place |
| Data quality | Great Expectations 40+ expectations — blocks pipeline on failure |
| CI/CD | GitHub Actions: dbt run → test → GE → docs on every push |
| Snowflake RBAC | DBT_ROLE (write), REGULATORY_ANALYST_ROLE (read only) |
| dbt lineage | Full audit trail from raw CSV to regulatory submission table |
| Macros | Reusable EL calculation, rating bucket, capital adequacy status |

---

## Tools Used

`dbt Core 1.7` · `Snowflake` · `Great Expectations` · `GitHub Actions`
`Python 3.11` · `SQL` · `dbt-utils`

---

## Datasets (all synthetic)

| Dataset | Rows |
|---|---|
| Loan portfolio | 1,500 |
| Counterparties | 200 |
| RWA components | 1,500 |
| Capital components | 45 |
| Stress scenarios | 900 |
| Market risk positions | 800 |
| **Total** | **4,945** |

---

## Resume Bullets

1. "Built dbt + Snowflake regulatory pipeline automating Basel III RWA, CCAR stress testing, and LCR/NSFR reporting — cutting quarterly prep from 3 weeks to 4 hours with full automated data lineage"
2. "Implemented Great Expectations validation suite with 40+ expectations blocking pipeline on source failures — ensuring zero defective data reaches regulatory submission tables"
3. "Designed GitHub Actions CI/CD running dbt build + test + GE validation on every PR — enforcing data quality gates before production deployment"

---

## 90-Second Interview Explanation

*"The business problem is classic in banking — regulatory teams spending weeks manually
assembling Basel III reports from multiple source systems, with no lineage and no
automated quality checks.*

*I built a dbt pipeline on Snowflake with three layers. Staging views clean and type
the raw source data. An intermediate table joins loan portfolio, counterparty, and RWA
data to create the enriched credit risk view — computed once, reused by three marts.
The mart tables are the actual regulatory submissions: Basel III RWA by asset class,
capital adequacy ratios versus regulatory minimums, CCAR stress test results across
three scenarios, and LCR and NSFR liquidity ratios.*

*The key detail is the dbt vars pattern — CET1 minimum ratio, conservation buffer,
LCR floor — all defined once in dbt_project.yml. Every compliance flag across every
model reads from those variables. When regulators change a threshold, one line changes
and every model updates on the next dbt run.*

*Great Expectations runs before data loads to Snowflake. If source data has PD values
outside 0 to 1, or duplicate loan IDs, or invalid scenario names — the pipeline stops.
Bad data never reaches the regulatory tables.*

*GitHub Actions runs the full CI pipeline on every pull request — dbt run, dbt test,
Great Expectations, dbt docs generate. A green checkmark means the code is safe to
merge. That is what I would bring to Morgan Stanley."*

---

*Portfolio project — Bhogya Swetha Malladi · Data Engineer · New York, NY*
