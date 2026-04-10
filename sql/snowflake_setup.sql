-- ============================================================
-- Snowflake Setup — dbt Regulatory Reporting Engine
-- Run this ONCE before first dbt run
-- ============================================================

USE ROLE SYSADMIN;

-- ── Warehouse ──────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS REGULATORY_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Regulatory reporting — sized for quarterly batch workloads';

-- ── Database and schemas ───────────────────────────────────────
CREATE DATABASE IF NOT EXISTS REGULATORY_DB;

CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.RAW;           -- source data
CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.STAGING;       -- dbt staging views
CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.INTERMEDIATE;  -- dbt intermediate tables
CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.CREDIT_RISK;   -- credit mart
CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.CAPITAL;       -- capital mart
CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.LIQUIDITY;     -- liquidity mart
CREATE SCHEMA IF NOT EXISTS REGULATORY_DB.AUDIT;         -- dbt run logs

-- ── RBAC ──────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS DBT_ROLE;
CREATE ROLE IF NOT EXISTS REGULATORY_ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS REGULATORY_READER_ROLE;

-- dbt role — needs full access to build models
GRANT USAGE  ON WAREHOUSE REGULATORY_WH      TO ROLE DBT_ROLE;
GRANT USAGE  ON DATABASE  REGULATORY_DB      TO ROLE DBT_ROLE;
GRANT ALL    ON ALL SCHEMAS IN DATABASE REGULATORY_DB TO ROLE DBT_ROLE;
GRANT ALL    ON FUTURE TABLES  IN DATABASE REGULATORY_DB TO ROLE DBT_ROLE;
GRANT ALL    ON FUTURE SCHEMAS IN DATABASE REGULATORY_DB TO ROLE DBT_ROLE;

-- Analyst role — read marts only
GRANT USAGE  ON WAREHOUSE REGULATORY_WH      TO ROLE REGULATORY_ANALYST_ROLE;
GRANT USAGE  ON DATABASE  REGULATORY_DB      TO ROLE REGULATORY_ANALYST_ROLE;
GRANT USAGE  ON SCHEMA REGULATORY_DB.CREDIT_RISK  TO ROLE REGULATORY_ANALYST_ROLE;
GRANT USAGE  ON SCHEMA REGULATORY_DB.CAPITAL      TO ROLE REGULATORY_ANALYST_ROLE;
GRANT USAGE  ON SCHEMA REGULATORY_DB.LIQUIDITY    TO ROLE REGULATORY_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA REGULATORY_DB.CREDIT_RISK TO ROLE REGULATORY_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA REGULATORY_DB.CAPITAL     TO ROLE REGULATORY_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA REGULATORY_DB.CREDIT_RISK TO ROLE REGULATORY_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA REGULATORY_DB.CAPITAL     TO ROLE REGULATORY_ANALYST_ROLE;

-- ── Raw tables — load CSVs via COPY INTO ──────────────────────
USE DATABASE REGULATORY_DB;
USE SCHEMA RAW;
USE WAREHOUSE REGULATORY_WH;

CREATE OR REPLACE TABLE RAW.LOAN_PORTFOLIO (
    loan_id             VARCHAR(20),
    counterparty_id     VARCHAR(20),
    asset_class         VARCHAR(50),
    product_type        VARCHAR(50),
    sector              VARCHAR(50),
    region              VARCHAR(50),
    country             VARCHAR(10),
    currency            VARCHAR(10),
    exposure_amount     FLOAT,
    outstanding_balance FLOAT,
    committed_amount    FLOAT,
    ead_estimate        FLOAT,
    collateral_value    FLOAT,
    internal_rating     VARCHAR(10),
    pd_estimate         FLOAT,
    lgd_estimate        FLOAT,
    collateral_type     VARCHAR(30),
    origination_date    DATE,
    maturity_date       DATE,
    is_defaulted        BOOLEAN,
    is_impaired         BOOLEAN,
    reporting_date      DATE,
    source_system       VARCHAR(30)
);

CREATE OR REPLACE TABLE RAW.COUNTERPARTIES (
    counterparty_id     VARCHAR(20),
    counterparty_name   VARCHAR(100),
    counterparty_type   VARCHAR(30),
    sector              VARCHAR(50),
    country             VARCHAR(10),
    region              VARCHAR(50),
    external_rating     VARCHAR(10),
    is_pep              BOOLEAN,
    is_sanctioned       BOOLEAN,
    kyc_status          VARCHAR(20),
    onboarding_date     DATE,
    last_review_date    DATE,
    credit_limit        FLOAT,
    reporting_date      DATE
);

CREATE OR REPLACE TABLE RAW.RWA_COMPONENTS (
    loan_id             VARCHAR(20),
    asset_class         VARCHAR(50),
    internal_rating     VARCHAR(10),
    risk_weight         FLOAT,
    ead                 FLOAT,
    credit_rwa          FLOAT,
    operational_rwa     FLOAT,
    market_rwa          FLOAT,
    total_rwa           FLOAT,
    capital_requirement FLOAT,
    approach            VARCHAR(30),
    reporting_date      DATE
);

CREATE OR REPLACE TABLE RAW.CAPITAL_COMPONENTS (
    entity_id           VARCHAR(20),
    reporting_period    VARCHAR(10),
    cet1_capital        FLOAT,
    additional_tier1    FLOAT,
    tier2_capital       FLOAT,
    total_capital       FLOAT,
    risk_weighted_assets FLOAT,
    cet1_ratio          FLOAT,
    tier1_ratio         FLOAT,
    total_capital_ratio FLOAT,
    leverage_ratio      FLOAT,
    lcr                 FLOAT,
    nsfr                FLOAT,
    reporting_date      DATE
);

CREATE OR REPLACE TABLE RAW.STRESS_SCENARIOS (
    scenario_id         VARCHAR(36),
    loan_id             VARCHAR(20),
    scenario_name       VARCHAR(30),
    scenario_year       INTEGER,
    stressed_pd         FLOAT,
    stressed_lgd        FLOAT,
    stressed_ead        FLOAT,
    expected_loss       FLOAT,
    stressed_rwa        FLOAT,
    capital_impact      FLOAT,
    reporting_date      DATE
);

-- ── Load data from CSV files (run after uploading CSVs to stage) ──
-- CREATE STAGE IF NOT EXISTS REGULATORY_DB.RAW.CSV_STAGE;
-- PUT file://data/loan_portfolio.csv @CSV_STAGE;
-- COPY INTO RAW.LOAN_PORTFOLIO FROM @CSV_STAGE/loan_portfolio.csv
--     FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 NULL_IF=(''));
