-- =============================================================================
-- CURATED LAYER DDL  (Star Schema + Snowflake Schema Dimensions)
-- E-Commerce Customer Behavior & Recommendation System
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE ECOM_DW;
CREATE SCHEMA IF NOT EXISTS ECOM_DW.CURATED;
USE SCHEMA ECOM_DW.CURATED;

-- ─── DIMENSION: DATE ───────────────────────────────────────────────────────
CREATE OR REPLACE TABLE DIM_DATE (
    date_key            INTEGER         PRIMARY KEY,   -- YYYYMMDD
    full_date           DATE            NOT NULL,
    year                SMALLINT        NOT NULL,
    quarter             TINYINT         NOT NULL,
    month               TINYINT         NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    week_of_year        TINYINT         NOT NULL,
    day_of_month        TINYINT         NOT NULL,
    day_of_week         TINYINT         NOT NULL,
    day_name            VARCHAR(20)     NOT NULL,
    is_weekend          BOOLEAN         NOT NULL,
    is_holiday          BOOLEAN         DEFAULT FALSE,
    fiscal_year         SMALLINT,
    fiscal_quarter      TINYINT
)
CLUSTER BY (year, month);

-- ─── DIMENSION: CUSTOMER (SCD Type 2) ─────────────────────────────────────
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_sk         INTEGER         AUTOINCREMENT PRIMARY KEY,
    customer_id         VARCHAR(50)     NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200),
    email               VARCHAR(200),
    phone               VARCHAR(30),
    date_of_birth       DATE,
    age_group           VARCHAR(20),
    gender              VARCHAR(10),
    country             VARCHAR(100),
    city                VARCHAR(100),
    zip_code            VARCHAR(20),
    registration_date   DATE,
    customer_segment    VARCHAR(50),
    -- SCD2 fields
    effective_start_date DATE           NOT NULL,
    effective_end_date   DATE,
    is_current           BOOLEAN        DEFAULT TRUE,
    -- Audit
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (country, customer_segment);

-- ─── DIMENSION: PRODUCT (Snowflake: linked to DIM_CATEGORY) ───────────────
CREATE OR REPLACE TABLE DIM_CATEGORY (
    category_sk         INTEGER         AUTOINCREMENT PRIMARY KEY,
    category_id         VARCHAR(50)     NOT NULL UNIQUE,
    category_name       VARCHAR(200)    NOT NULL,
    subcategory_name    VARCHAR(200),
    department          VARCHAR(200),
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_sk          INTEGER         AUTOINCREMENT PRIMARY KEY,
    product_id          VARCHAR(50)     NOT NULL,
    product_name        VARCHAR(500),
    category_sk         INTEGER         REFERENCES DIM_CATEGORY(category_sk),
    brand               VARCHAR(200),
    sku                 VARCHAR(100),
    unit_price          FLOAT,
    cost_price          FLOAT,
    gross_margin_pct    FLOAT,
    weight_kg           FLOAT,
    is_active           BOOLEAN         DEFAULT TRUE,
    launch_date         DATE,
    -- SCD2
    effective_start_date DATE           NOT NULL,
    effective_end_date   DATE,
    is_current           BOOLEAN        DEFAULT TRUE,
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (brand, is_active);

-- ─── DIMENSION: PAYMENT METHOD ─────────────────────────────────────────────
CREATE OR REPLACE TABLE DIM_PAYMENT_METHOD (
    payment_method_sk   INTEGER         AUTOINCREMENT PRIMARY KEY,
    payment_method      VARCHAR(100)    NOT NULL,
    payment_type        VARCHAR(50),     -- card / wallet / bnpl / crypto
    gateway             VARCHAR(100),
    is_digital          BOOLEAN
);

-- ─── DIMENSION: CHANNEL / GEOGRAPHY ───────────────────────────────────────
CREATE OR REPLACE TABLE DIM_CHANNEL (
    channel_sk          INTEGER         AUTOINCREMENT PRIMARY KEY,
    channel_name        VARCHAR(100)    NOT NULL,
    channel_type        VARCHAR(50),    -- web / mobile / marketplace
    platform            VARCHAR(100)
);

CREATE OR REPLACE TABLE DIM_GEOGRAPHY (
    geo_sk              INTEGER         AUTOINCREMENT PRIMARY KEY,
    country             VARCHAR(100),
    region              VARCHAR(100),
    city                VARCHAR(100),
    zip_code            VARCHAR(20),
    latitude            FLOAT,
    longitude           FLOAT
);

-- ─── FACT: ORDERS ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FACT_ORDERS (
    order_sk            INTEGER         AUTOINCREMENT PRIMARY KEY,
    order_id            VARCHAR(50)     NOT NULL,
    customer_sk         INTEGER         REFERENCES DIM_CUSTOMER(customer_sk),
    order_date_key      INTEGER         REFERENCES DIM_DATE(date_key),
    channel_sk          INTEGER         REFERENCES DIM_CHANNEL(channel_sk),
    geo_sk              INTEGER         REFERENCES DIM_GEOGRAPHY(geo_sk),
    order_status        VARCHAR(50),
    shipping_method     VARCHAR(100),
    gross_revenue       FLOAT,
    discount_amount     FLOAT,
    shipping_cost       FLOAT,
    tax_amount          FLOAT,
    net_revenue         FLOAT,
    item_count          INTEGER,
    currency            VARCHAR(10),
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (order_date_key, customer_sk)
PARTITION BY (order_date_key);

-- ─── FACT: ORDER ITEMS ─────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FACT_ORDER_ITEMS (
    order_item_sk       INTEGER         AUTOINCREMENT PRIMARY KEY,
    order_item_id       VARCHAR(50),
    order_sk            INTEGER         REFERENCES FACT_ORDERS(order_sk),
    order_id            VARCHAR(50),
    product_sk          INTEGER         REFERENCES DIM_PRODUCT(product_sk),
    order_date_key      INTEGER         REFERENCES DIM_DATE(date_key),
    customer_sk         INTEGER,
    quantity            INTEGER,
    unit_price          FLOAT,
    discount_pct        FLOAT,
    line_revenue        FLOAT,
    line_cost           FLOAT,
    line_margin         FLOAT,
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (order_date_key, product_sk);

-- ─── FACT: PAYMENTS ────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FACT_PAYMENTS (
    payment_sk          INTEGER         AUTOINCREMENT PRIMARY KEY,
    payment_id          VARCHAR(50),
    order_sk            INTEGER         REFERENCES FACT_ORDERS(order_sk),
    order_id            VARCHAR(50),
    payment_date_key    INTEGER         REFERENCES DIM_DATE(date_key),
    payment_method_sk   INTEGER         REFERENCES DIM_PAYMENT_METHOD(payment_method_sk),
    payment_status      VARCHAR(50),
    amount              FLOAT,
    transaction_ref     VARCHAR(200),
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (payment_date_key);

-- ─── FACT: CLICKSTREAM ─────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FACT_CLICKSTREAM (
    click_sk            INTEGER         AUTOINCREMENT PRIMARY KEY,
    event_id            VARCHAR(100),
    session_id          VARCHAR(100),
    customer_sk         INTEGER         REFERENCES DIM_CUSTOMER(customer_sk),
    product_sk          INTEGER         REFERENCES DIM_PRODUCT(product_sk),
    event_date_key      INTEGER         REFERENCES DIM_DATE(date_key),
    event_type          VARCHAR(100),   -- page_view / add_to_cart / purchase / search
    page_url            VARCHAR(1000),
    device_type         VARCHAR(50),
    browser             VARCHAR(100),
    referrer            VARCHAR(500),
    event_timestamp     TIMESTAMP_NTZ,
    raw_payload         VARIANT         -- Semi-structured data
)
CLUSTER BY (event_date_key, event_type)
PARTITION BY (event_date_key);

-- ─── FACT: REVIEWS ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FACT_REVIEWS (
    review_sk           INTEGER         AUTOINCREMENT PRIMARY KEY,
    review_id           VARCHAR(50),
    product_sk          INTEGER         REFERENCES DIM_PRODUCT(product_sk),
    customer_sk         INTEGER         REFERENCES DIM_CUSTOMER(customer_sk),
    review_date_key     INTEGER         REFERENCES DIM_DATE(date_key),
    rating              TINYINT,
    review_title        VARCHAR(500),
    review_body         VARCHAR(4000),
    helpful_votes       INTEGER,
    verified_purchase   BOOLEAN,
    sentiment_score     FLOAT,          -- populated by ML pipeline
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (review_date_key, product_sk);

-- ─── INDEXES (Search Optimization) ────────────────────────────────────────
ALTER TABLE DIM_CUSTOMER ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, email);
ALTER TABLE DIM_PRODUCT  ADD SEARCH OPTIMIZATION ON EQUALITY(product_id, sku);
ALTER TABLE FACT_ORDERS  ADD SEARCH OPTIMIZATION ON EQUALITY(order_id);
