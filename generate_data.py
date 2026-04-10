"""
Synthetic Data Generator — dbt + Snowflake Regulatory Reporting Engine
Generates realistic banking datasets for Basel III / CCAR reporting demos.
Run: python generate_data.py
Output: data/*.csv (6 files, ~4,000 total rows)

IMPORTANT: All data is 100% synthetic — randomly generated.
No real bank data, no real customer data, no real exposures.
"""

import csv, random, uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "data"
OUT.mkdir(exist_ok=True)

def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end-start).days))

COUNTERPARTIES = [f"CPTY-{i:04d}" for i in range(1, 201)]
ASSET_CLASSES  = ["Corporate Loan","Retail Mortgage","SME Loan","Sovereign Bond",
                   "Credit Card","Auto Loan","Commercial Real Estate","Interbank"]
RATINGS        = ["AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","BBB-","BB+","BB","BB-","B","CCC"]
RATING_WEIGHTS = [.05,.06,.07,.07,.08,.08,.07,.08,.08,.07,.06,.05,.05,.04,.05]
SECTORS        = ["Financial","Technology","Energy","Healthcare","Consumer","Industrial",
                  "Real Estate","Government","Utilities","Materials"]
CURRENCIES     = ["USD","EUR","GBP","JPY","CHF"]
CURR_WEIGHTS   = [.65,.15,.08,.07,.05]
REGIONS        = ["North America","Europe","Asia Pacific","Latin America","Middle East"]
PRODUCTS       = ["Term Loan","Revolving Credit","Mortgage","Bond","Derivative","Deposit"]

# ── 1. loan_portfolio (1,500 rows) ─────────────────────────────────────────────
loans = []
for i in range(1, 1501):
    asset_class = random.choice(ASSET_CLASSES)
    rating      = random.choices(RATINGS, weights=RATING_WEIGHTS)[0]
    currency    = random.choices(CURRENCIES, weights=CURR_WEIGHTS)[0]
    exposure    = round(random.uniform(100_000, 50_000_000), 2)
    lgd         = round(random.uniform(0.15, 0.75), 4)
    pd          = round(random.uniform(0.001, 0.25), 4)
    maturity    = rand_date(datetime(2024,1,1), datetime(2035,12,31))
    origination = rand_date(datetime(2018,1,1), datetime(2024,1,1))
    loans.append({
        "loan_id":            f"LN-{i:06d}",
        "counterparty_id":    random.choice(COUNTERPARTIES),
        "asset_class":        asset_class,
        "product_type":       random.choice(PRODUCTS),
        "sector":             random.choice(SECTORS),
        "region":             random.choice(REGIONS),
        "country":            random.choice(["US","UK","DE","FR","JP","SG","AU","CA","CH","HK"]),
        "currency":           currency,
        "exposure_amount":    exposure,
        "outstanding_balance":round(exposure * random.uniform(0.5, 1.0), 2),
        "committed_amount":   round(exposure * random.uniform(1.0, 1.2), 2),
        "internal_rating":    rating,
        "pd_estimate":        pd,
        "lgd_estimate":       lgd,
        "ead_estimate":       round(exposure * random.uniform(0.7, 1.0), 2),
        "maturity_date":      maturity.strftime("%Y-%m-%d"),
        "origination_date":   origination.strftime("%Y-%m-%d"),
        "is_defaulted":       random.choices([True,False], weights=[.03,.97])[0],
        "is_impaired":        random.choices([True,False], weights=[.05,.95])[0],
        "collateral_type":    random.choice(["Real Estate","Cash","Securities","Guarantee","None"]),
        "collateral_value":   round(exposure * random.uniform(0, 1.5), 2),
        "reporting_date":     "2024-03-31",
        "source_system":      random.choice(["LoanIQ","Murex","Finastra","Manual"]),
    })
with open(OUT/"loan_portfolio.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=loans[0].keys()); w.writeheader(); w.writerows(loans)
print(f"loan_portfolio.csv        → {len(loans):,} rows")

# ── 2. counterparties (200 rows) ───────────────────────────────────────────────
counterparties = []
for cid in COUNTERPARTIES:
    counterparties.append({
        "counterparty_id":      cid,
        "counterparty_name":    f"Counterparty {cid}",
        "counterparty_type":    random.choice(["Corporate","Financial Institution","Sovereign","Retail","SME"]),
        "sector":               random.choice(SECTORS),
        "country":              random.choice(["US","UK","DE","FR","JP","SG","AU","CA","CH","HK"]),
        "region":               random.choice(REGIONS),
        "external_rating":      random.choices(RATINGS, weights=RATING_WEIGHTS)[0],
        "is_pep":               random.choices([True,False], weights=[.02,.98])[0],
        "is_sanctioned":        random.choices([True,False], weights=[.005,.995])[0],
        "kyc_status":           random.choices(["Verified","Pending","Failed"], weights=[.90,.08,.02])[0],
        "onboarding_date":      rand_date(datetime(2010,1,1), datetime(2023,1,1)).strftime("%Y-%m-%d"),
        "last_review_date":     rand_date(datetime(2022,1,1), datetime(2024,1,1)).strftime("%Y-%m-%d"),
        "credit_limit":         round(random.uniform(1_000_000, 500_000_000), 2),
        "reporting_date":       "2024-03-31",
    })
with open(OUT/"counterparties.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=counterparties[0].keys()); w.writeheader(); w.writerows(counterparties)
print(f"counterparties.csv        → {len(counterparties):,} rows")

# ── 3. market_risk_positions (800 rows) ────────────────────────────────────────
positions = []
for i in range(1, 801):
    notional  = round(random.uniform(100_000, 100_000_000), 2)
    mtm_value = round(notional * random.uniform(-0.2, 0.3), 2)
    positions.append({
        "position_id":      f"POS-{i:06d}",
        "desk":             random.choice(["Rates","FX","Credit","Equity","Commodities"]),
        "instrument_type":  random.choice(["IRS","CDS","FX Forward","Equity Option","Bond","Future"]),
        "currency":         random.choices(CURRENCIES, weights=CURR_WEIGHTS)[0],
        "notional_amount":  notional,
        "mtm_value":        mtm_value,
        "var_1d_99":        round(abs(mtm_value) * random.uniform(0.01, 0.08), 2),
        "var_10d_99":       round(abs(mtm_value) * random.uniform(0.03, 0.15), 2),
        "stressed_var":     round(abs(mtm_value) * random.uniform(0.05, 0.25), 2),
        "delta":            round(random.uniform(-1, 1), 4),
        "gamma":            round(random.uniform(-0.5, 0.5), 6),
        "vega":             round(random.uniform(-100000, 100000), 2),
        "maturity_bucket":  random.choice(["0-1Y","1-3Y","3-5Y","5-10Y","10Y+"]),
        "reporting_date":   "2024-03-31",
        "book":             random.choice(["Trading","Banking","AFS","HTM"]),
    })
with open(OUT/"market_risk_positions.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=positions[0].keys()); w.writeheader(); w.writerows(positions)
print(f"market_risk_positions.csv → {len(positions):,} rows")

# ── 4. capital_components (50 rows — one per reporting period/entity) ──────────
capital = []
entities = [f"ENTITY-{i:02d}" for i in range(1, 11)]
periods  = ["2022-Q1","2022-Q2","2022-Q3","2022-Q4",
            "2023-Q1","2023-Q2","2023-Q3","2023-Q4","2024-Q1"]
for entity in entities[:5]:
    for period in periods:
        cet1  = round(random.uniform(5_000_000_000, 50_000_000_000), 2)
        at1   = round(cet1 * random.uniform(0.05, 0.15), 2)
        t2    = round(cet1 * random.uniform(0.08, 0.20), 2)
        rwa   = round(cet1 * random.uniform(8, 15), 2)
        capital.append({
            "entity_id":            entity,
            "reporting_period":     period,
            "cet1_capital":         cet1,
            "additional_tier1":     at1,
            "tier2_capital":        t2,
            "total_capital":        round(cet1 + at1 + t2, 2),
            "risk_weighted_assets": rwa,
            "cet1_ratio":           round(cet1 / rwa * 100, 4),
            "tier1_ratio":          round((cet1 + at1) / rwa * 100, 4),
            "total_capital_ratio":  round((cet1 + at1 + t2) / rwa * 100, 4),
            "leverage_ratio":       round(random.uniform(3.5, 8.0), 4),
            "lcr":                  round(random.uniform(110, 200), 2),
            "nsfr":                 round(random.uniform(105, 150), 2),
            "reporting_date":       "2024-03-31",
        })
with open(OUT/"capital_components.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=capital[0].keys()); w.writeheader(); w.writerows(capital)
print(f"capital_components.csv    → {len(capital):,} rows")

# ── 5. stress_scenarios (300 rows) ────────────────────────────────────────────
scenarios = []
scenario_names = ["Baseline","Adverse","Severely Adverse"]
for loan in random.sample(loans, 300):
    for scenario in scenario_names:
        stressed_pd  = loan["pd_estimate"] * (1 + random.uniform(0, 5) if scenario != "Baseline" else 1)
        stressed_lgd = min(loan["lgd_estimate"] * (1 + random.uniform(0, 0.5) if scenario != "Baseline" else 1), 1.0)
        scenarios.append({
            "scenario_id":       str(uuid.uuid4()),
            "loan_id":           loan["loan_id"],
            "scenario_name":     scenario,
            "scenario_year":     random.choice([1, 2, 3]),
            "stressed_pd":       round(min(stressed_pd, 1.0), 4),
            "stressed_lgd":      round(stressed_lgd, 4),
            "stressed_ead":      round(loan["ead_estimate"] * random.uniform(0.9, 1.1), 2),
            "expected_loss":     round(loan["ead_estimate"] * stressed_pd * stressed_lgd, 2),
            "stressed_rwa":      round(loan["ead_estimate"] * random.uniform(0.5, 1.5), 2),
            "capital_impact":    round(loan["ead_estimate"] * stressed_pd * stressed_lgd * random.uniform(0.08, 0.12), 2),
            "reporting_date":    "2024-03-31",
        })
with open(OUT/"stress_scenarios.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=scenarios[0].keys()); w.writeheader(); w.writerows(scenarios)
print(f"stress_scenarios.csv      → {len(scenarios):,} rows")

# ── 6. rwa_components (1,500 rows — matches loan_portfolio) ───────────────────
# Basel III standardized approach RWA calculation
RW_MAP = {
    "AAA": 0.20, "AA+": 0.20, "AA": 0.20, "AA-": 0.20,
    "A+": 0.50,  "A": 0.50,   "A-": 0.50,
    "BBB+": 1.00,"BBB": 1.00, "BBB-": 1.00,
    "BB+": 1.50, "BB": 1.50,  "BB-": 1.50,
    "B": 1.50,   "CCC": 1.50,
}
rwa_rows = []
for loan in loans:
    rw        = RW_MAP.get(loan["internal_rating"], 1.0)
    credit_rwa = round(loan["ead_estimate"] * rw, 2)
    op_rwa     = round(loan["ead_estimate"] * 0.12, 2)
    mkt_rwa    = round(loan["ead_estimate"] * random.uniform(0.05, 0.15), 2)
    rwa_rows.append({
        "loan_id":             loan["loan_id"],
        "asset_class":         loan["asset_class"],
        "internal_rating":     loan["internal_rating"],
        "risk_weight":         rw,
        "ead":                 loan["ead_estimate"],
        "credit_rwa":          credit_rwa,
        "operational_rwa":     op_rwa,
        "market_rwa":          mkt_rwa,
        "total_rwa":           round(credit_rwa + op_rwa + mkt_rwa, 2),
        "capital_requirement": round((credit_rwa + op_rwa + mkt_rwa) * 0.08, 2),
        "approach":            random.choice(["Standardized","IRB Foundation","IRB Advanced"]),
        "reporting_date":      "2024-03-31",
    })
with open(OUT/"rwa_components.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=rwa_rows[0].keys()); w.writeheader(); w.writerows(rwa_rows)
print(f"rwa_components.csv        → {len(rwa_rows):,} rows")

total = sum([len(loans),len(counterparties),len(positions),len(capital),len(scenarios),len(rwa_rows)])
print(f"\nTotal rows: {total:,} across 6 files")
print(f"Output:     {OUT.resolve()}")
