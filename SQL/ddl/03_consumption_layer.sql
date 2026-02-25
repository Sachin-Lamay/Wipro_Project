-- =============================================================================
-- CONSUMPTION LAYER DDL
-- Aggregated / denormalized tables for BI & Recommendation serving
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE ECOM_DW;
CREATE SCHEMA IF NOT EXISTS ECOM_DW.CONSUMPTION;
USE SCHEMA ECOM_DW.CONSUMPTION;

-- ─── KPI: DAILY REVENUE SUMMARY ───────────────────────────────────────────
CREATE OR REPLACE TABLE AGG_DAILY_REVENUE (
    date_key            INTEGER,
    report_date         DATE,
    channel             VARCHAR(100),
    country             VARCHAR(100),
    total_orders        INTEGER,
    total_revenue       FLOAT,
    total_discounts     FLOAT,
    total_tax           FLOAT,
    net_revenue         FLOAT,
    avg_order_value     FLOAT,
    new_customers       INTEGER,
    returning_customers INTEGER,
    conversion_rate     FLOAT,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date_key);

-- ─── KPI: PRODUCT PERFORMANCE ─────────────────────────────────────────────
CREATE OR REPLACE TABLE AGG_PRODUCT_PERFORMANCE (
    report_month        VARCHAR(7),     -- YYYY-MM
    product_id          VARCHAR(50),
    product_name        VARCHAR(500),
    category_name       VARCHAR(200),
    brand               VARCHAR(200),
    units_sold          INTEGER,
    total_revenue       FLOAT,
    total_cost          FLOAT,
    gross_margin        FLOAT,
    gross_margin_pct    FLOAT,
    avg_rating          FLOAT,
    review_count        INTEGER,
    return_rate         FLOAT,
    page_views          BIGINT,
    add_to_cart_count   INTEGER,
    conversion_rate     FLOAT,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ─── KPI: CUSTOMER RFM ────────────────────────────────────────────────────
CREATE OR REPLACE TABLE AGG_CUSTOMER_RFM (
    customer_id         VARCHAR(50),
    customer_name       VARCHAR(200),
    email               VARCHAR(200),
    country             VARCHAR(100),
    segment             VARCHAR(50),
    recency_days        INTEGER,
    frequency           INTEGER,
    monetary            FLOAT,
    r_score             TINYINT,
    f_score             TINYINT,
    m_score             TINYINT,
    rfm_score           VARCHAR(3),
    rfm_segment         VARCHAR(50),    -- Champions / Loyal / At Risk / etc.
    clv_predicted       FLOAT,
    first_order_date    DATE,
    last_order_date     DATE,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (rfm_segment);

-- ─── KPI: CUSTOMER LIFETIME VALUE ─────────────────────────────────────────
CREATE OR REPLACE TABLE AGG_CLV (
    customer_id         VARCHAR(50),
    total_revenue       FLOAT,
    total_orders        INTEGER,
    avg_order_value     FLOAT,
    purchase_frequency  FLOAT,
    avg_lifespan_days   INTEGER,
    historical_clv      FLOAT,
    predicted_clv_90d   FLOAT,
    predicted_clv_365d  FLOAT,
    clv_tier            VARCHAR(20),    -- Platinum / Gold / Silver / Bronze
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ─── RECOMMENDATION: PRODUCT AFFINITY (co-purchase) ──────────────────────
CREATE OR REPLACE TABLE REC_PRODUCT_AFFINITY (
    product_a_id        VARCHAR(50),
    product_b_id        VARCHAR(50),
    co_purchase_count   INTEGER,
    support             FLOAT,
    confidence          FLOAT,
    lift                FLOAT,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (product_a_id);

-- ─── RECOMMENDATION: USER-ITEM SCORES ─────────────────────────────────────
CREATE OR REPLACE TABLE REC_USER_ITEM_SCORES (
    customer_id         VARCHAR(50),
    product_id          VARCHAR(50),
    score               FLOAT,
    model_version       VARCHAR(50),
    algorithm           VARCHAR(50),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (customer_id);

-- ─── RECOMMENDATION: TOP PICKS PER CUSTOMER ───────────────────────────────
CREATE OR REPLACE TABLE REC_TOP_PICKS (
    customer_id         VARCHAR(50),
    rank                TINYINT,
    product_id          VARCHAR(50),
    product_name        VARCHAR(500),
    category_name       VARCHAR(200),
    score               FLOAT,
    reason              VARCHAR(200),  -- "frequently bought together" etc.
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ─── MATERIALIZED VIEW: REAL-TIME FUNNEL ──────────────────────────────────
-- (Snowflake dynamic table approximation)
CREATE OR REPLACE VIEW VW_CONVERSION_FUNNEL AS
SELECT
    DATE_TRUNC('day', fc.event_timestamp)  AS event_date,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'page_view'   THEN fc.session_id END) AS page_views,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'add_to_cart' THEN fc.session_id END) AS add_to_cart,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'checkout'    THEN fc.session_id END) AS checkouts,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'purchase'    THEN fc.session_id END) AS purchases,
    ROUND(
        COUNT(DISTINCT CASE WHEN fc.event_type = 'purchase' THEN fc.session_id END)::FLOAT /
        NULLIF(COUNT(DISTINCT CASE WHEN fc.event_type = 'page_view' THEN fc.session_id END),0) * 100,
    2) AS overall_conversion_pct
FROM ECOM_DW.CURATED.FACT_CLICKSTREAM fc
GROUP BY 1
ORDER BY 1 DESC;
