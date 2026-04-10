"""
Great Expectations Validation Suite
Project: dbt + Snowflake Regulatory Reporting Engine

Validates source CSV data BEFORE loading to Snowflake.
Run: python great_expectations/run_validations.py

In production: runs in GitHub Actions after data lands in Snowflake raw schema.
Any failed expectation blocks the dbt pipeline from running.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_PATH    = Path(__file__).parent.parent / "data"
RESULTS_PATH = Path(__file__).parent / "results"
RESULTS_PATH.mkdir(exist_ok=True)

# ── Simple expectation engine (no GE cloud needed for portfolio demo) ──────────
class ExpectationSuite:
    def __init__(self, name: str, df: pd.DataFrame):
        self.name    = name
        self.df      = df
        self.results = []
        self.passed  = 0
        self.failed  = 0

    def expect_column_values_to_not_be_null(self, col: str, threshold: float = 0.0):
        null_pct = self.df[col].isna().mean() * 100
        passed   = null_pct <= threshold
        self._record(f"not_null({col})", passed,
                     f"Null pct: {null_pct:.2f}% (max allowed: {threshold}%)")

    def expect_column_values_to_be_unique(self, col: str):
        dup_count = self.df.duplicated(subset=[col]).sum()
        self._record(f"unique({col})", dup_count == 0,
                     f"Duplicate count: {dup_count}")

    def expect_column_values_to_be_between(self, col: str, min_val, max_val, threshold: float = 0.0):
        series   = pd.to_numeric(self.df[col], errors="coerce")
        oob      = ((series < min_val) | (series > max_val)).mean() * 100
        passed   = oob <= threshold
        self._record(f"between({col}, {min_val}, {max_val})", passed,
                     f"Out-of-range pct: {oob:.2f}% (max allowed: {threshold}%)")

    def expect_column_values_to_be_in_set(self, col: str, value_set: list):
        invalid  = ~self.df[col].isin(value_set)
        inv_count = invalid.sum()
        self._record(f"in_set({col})", inv_count == 0,
                     f"Invalid values: {inv_count} | Examples: {self.df[col][invalid].unique()[:3].tolist()}")

    def expect_column_values_to_be_positive(self, col: str, threshold: float = 0.0):
        series  = pd.to_numeric(self.df[col], errors="coerce")
        neg_pct = (series <= 0).mean() * 100
        passed  = neg_pct <= threshold
        self._record(f"positive({col})", passed,
                     f"Non-positive pct: {neg_pct:.2f}%")

    def expect_table_row_count_to_be_between(self, min_rows: int, max_rows: int):
        count  = len(self.df)
        passed = min_rows <= count <= max_rows
        self._record(f"row_count_between({min_rows}, {max_rows})", passed,
                     f"Actual row count: {count}")

    def expect_column_pair_values_a_to_be_less_than_b(self, col_a: str, col_b: str, threshold: float = 0.0):
        a = pd.to_numeric(self.df[col_a], errors="coerce")
        b = pd.to_numeric(self.df[col_b], errors="coerce")
        violations = (a >= b).mean() * 100
        passed = violations <= threshold
        self._record(f"{col_a}_less_than_{col_b}", passed,
                     f"Violation pct: {violations:.2f}%")

    def _record(self, check: str, passed: bool, detail: str):
        status = "PASS" if passed else "FAIL"
        if passed: self.passed += 1
        else:      self.failed += 1
        self.results.append({
            "suite": self.name, "check": check,
            "status": status, "detail": detail
        })
        icon = "PASS" if passed else "FAIL"
        print(f"  [{icon}] {check:<55} {detail}")

    def summary(self):
        return {"suite": self.name, "passed": self.passed, "failed": self.failed,
                "total": self.passed + self.failed}


# ── Run validations ────────────────────────────────────────────────────────────

all_results = []
all_summaries = []

# ── 1. Loan Portfolio ──────────────────────────────────────────────────────────
print("\n=== loan_portfolio ===")
loans = pd.read_csv(DATA_PATH / "loan_portfolio.csv")
suite = ExpectationSuite("loan_portfolio", loans)

suite.expect_table_row_count_to_be_between(100, 10_000)
suite.expect_column_values_to_not_be_null("loan_id")
suite.expect_column_values_to_not_be_null("counterparty_id")
suite.expect_column_values_to_not_be_null("exposure_amount")
suite.expect_column_values_to_not_be_null("reporting_date")
suite.expect_column_values_to_be_unique("loan_id")
suite.expect_column_values_to_be_positive("exposure_amount")
suite.expect_column_values_to_be_positive("ead_estimate")
suite.expect_column_values_to_be_between("pd_estimate", 0, 1)
suite.expect_column_values_to_be_between("lgd_estimate", 0, 1)
suite.expect_column_values_to_be_in_set("asset_class", [
    "Corporate Loan","Retail Mortgage","SME Loan","Sovereign Bond",
    "Credit Card","Auto Loan","Commercial Real Estate","Interbank"
])
suite.expect_column_values_to_be_in_set("currency", ["USD","EUR","GBP","JPY","CHF"])
suite.expect_column_pair_values_a_to_be_less_than_b("outstanding_balance","committed_amount", threshold=5.0)

all_results.extend(suite.results)
all_summaries.append(suite.summary())

# ── 2. Counterparties ──────────────────────────────────────────────────────────
print("\n=== counterparties ===")
cpty = pd.read_csv(DATA_PATH / "counterparties.csv")
suite = ExpectationSuite("counterparties", cpty)

suite.expect_table_row_count_to_be_between(10, 10_000)
suite.expect_column_values_to_not_be_null("counterparty_id")
suite.expect_column_values_to_be_unique("counterparty_id")
suite.expect_column_values_to_not_be_null("kyc_status")
suite.expect_column_values_to_be_in_set("kyc_status", ["Verified","Pending","Failed"])
suite.expect_column_values_to_be_positive("credit_limit")
suite.expect_column_values_to_be_in_set("counterparty_type", [
    "Corporate","Financial Institution","Sovereign","Retail","SME"
])

all_results.extend(suite.results)
all_summaries.append(suite.summary())

# ── 3. RWA Components ─────────────────────────────────────────────────────────
print("\n=== rwa_components ===")
rwa = pd.read_csv(DATA_PATH / "rwa_components.csv")
suite = ExpectationSuite("rwa_components", rwa)

suite.expect_table_row_count_to_be_between(100, 10_000)
suite.expect_column_values_to_not_be_null("loan_id")
suite.expect_column_values_to_not_be_null("total_rwa")
suite.expect_column_values_to_be_between("risk_weight", 0, 2.0)
suite.expect_column_values_to_be_positive("ead")
suite.expect_column_values_to_be_in_set("approach", [
    "Standardized","IRB Foundation","IRB Advanced"
])

all_results.extend(suite.results)
all_summaries.append(suite.summary())

# ── 4. Capital Components ─────────────────────────────────────────────────────
print("\n=== capital_components ===")
capital = pd.read_csv(DATA_PATH / "capital_components.csv")
suite = ExpectationSuite("capital_components", capital)

suite.expect_table_row_count_to_be_between(1, 500)
suite.expect_column_values_to_not_be_null("entity_id")
suite.expect_column_values_to_not_be_null("reporting_period")
suite.expect_column_values_to_be_positive("cet1_capital")
suite.expect_column_values_to_be_positive("risk_weighted_assets")
suite.expect_column_values_to_be_between("cet1_ratio", 0, 50)
suite.expect_column_values_to_be_between("lcr", 50, 500)
suite.expect_column_values_to_be_between("nsfr", 50, 300)
suite.expect_column_pair_values_a_to_be_less_than_b("cet1_capital","total_capital")

all_results.extend(suite.results)
all_summaries.append(suite.summary())

# ── 5. Stress Scenarios ────────────────────────────────────────────────────────
print("\n=== stress_scenarios ===")
stress = pd.read_csv(DATA_PATH / "stress_scenarios.csv")
suite = ExpectationSuite("stress_scenarios", stress)

suite.expect_table_row_count_to_be_between(100, 100_000)
suite.expect_column_values_to_not_be_null("scenario_id")
suite.expect_column_values_to_be_unique("scenario_id")
suite.expect_column_values_to_be_in_set("scenario_name", [
    "Baseline","Adverse","Severely Adverse"
])
suite.expect_column_values_to_be_between("stressed_pd", 0, 1)
suite.expect_column_values_to_be_between("stressed_lgd", 0, 1)
suite.expect_column_values_to_be_positive("expected_loss")

all_results.extend(suite.results)
all_summaries.append(suite.summary())

# ── Final summary ──────────────────────────────────────────────────────────────
total_pass = sum(s["passed"] for s in all_summaries)
total_fail = sum(s["failed"] for s in all_summaries)
total_all  = total_pass + total_fail

print(f"\n{'='*55}")
print(f"GREAT EXPECTATIONS VALIDATION SUMMARY")
print(f"{'='*55}")
for s in all_summaries:
    status = "PASS" if s["failed"] == 0 else "FAIL"
    print(f"  [{status}] {s['suite']:<30} {s['passed']:>3} pass / {s['failed']:>2} fail")
print(f"{'─'*55}")
print(f"  TOTAL                               {total_pass:>3} pass / {total_fail:>2} fail")
print(f"  Pass rate: {total_pass/total_all*100:.1f}%")
print(f"{'='*55}")

# Save results
results_file = RESULTS_PATH / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(results_file, "w") as f:
    json.dump({"summary": all_summaries, "details": all_results,
               "run_ts": datetime.now().isoformat()}, f, indent=2)
print(f"\nResults saved: {results_file}")

if total_fail > 0:
    raise SystemExit(f"VALIDATION FAILED — {total_fail} expectations not met. Review results before loading to Snowflake.")
