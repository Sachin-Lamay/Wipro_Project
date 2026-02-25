-- =============================================================================
-- STAGE LAYER DDL
-- E-Commerce Customer Behavior & Recommendation System
-- Layer: Raw ingestion from CSV/JSON sources
-- =============================================================================

USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS ECOM_DW;
CREATE SCHEMA IF NOT EXISTS ECOM_DW.STAGE;
USE SCHEMA ECOM_DW.STAGE;

-- ─── FILE FORMATS ──────────────────────────────────────────────────────────
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- ─── INTERNAL STAGES ───────────────────────────────────────────────────────
CREATE OR REPLACE STAGE stg_customers_stage  FILE_FORMAT = csv_format;
CREATE OR REPLACE STAGE stg_products_stage   FILE_FORMAT = csv_format;
CREATE OR REPLACE STAGE stg_orders_stage     FILE_FORMAT = csv_format;
CREATE OR REPLACE STAGE stg_payments_stage   FILE_FORMAT = csv_format;
CREATE OR REPLACE STAGE stg_clickstream_stage FILE_FORMAT = json_format;
CREATE OR REPLACE STAGE stg_reviews_stage    FILE_FORMAT = csv_format;

-- ─── STAGE TABLES ──────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STG_CUSTOMERS (
    customer_id         VARCHAR(50),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(200),
    phone               VARCHAR(30),
    date_of_birth       DATE,
    gender              VARCHAR(10),
    country             VARCHAR(100),
    city                VARCHAR(100),
    zip_code            VARCHAR(20),
    registration_date   TIMESTAMP_NTZ,
    segment             VARCHAR(50),
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         INTEGER
);

CREATE OR REPLACE TABLE STG_PRODUCTS (
    product_id          VARCHAR(50),
    product_name        VARCHAR(500),
    category_id         VARCHAR(50),
    category_name       VARCHAR(200),
    subcategory_name    VARCHAR(200),
    brand               VARCHAR(200),
    sku                 VARCHAR(100),
    unit_price          FLOAT,
    cost_price          FLOAT,
    weight_kg           FLOAT,
    is_active           BOOLEAN,
    launch_date         DATE,
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
);

CREATE OR REPLACE TABLE STG_ORDERS (
    order_id            VARCHAR(50),
    customer_id         VARCHAR(50),
    order_date          TIMESTAMP_NTZ,
    status              VARCHAR(50),
    shipping_method     VARCHAR(100),
    shipping_cost       FLOAT,
    discount_amount     FLOAT,
    tax_amount          FLOAT,
    total_amount        FLOAT,
    currency            VARCHAR(10),
    channel             VARCHAR(50),
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
);

CREATE OR REPLACE TABLE STG_ORDER_ITEMS (
    order_item_id       VARCHAR(50),
    order_id            VARCHAR(50),
    product_id          VARCHAR(50),
    quantity            INTEGER,
    unit_price          FLOAT,
    discount_pct        FLOAT,
    line_total          FLOAT,
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
);

CREATE OR REPLACE TABLE STG_PAYMENTS (
    payment_id          VARCHAR(50),
    order_id            VARCHAR(50),
    payment_date        TIMESTAMP_NTZ,
    payment_method      VARCHAR(100),
    payment_status      VARCHAR(50),
    amount              FLOAT,
    gateway             VARCHAR(100),
    transaction_ref     VARCHAR(200),
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
);

CREATE OR REPLACE TABLE STG_CLICKSTREAM (
    event_id            VARCHAR(100),
    session_id          VARCHAR(100),
    customer_id         VARCHAR(50),
    event_type          VARCHAR(100),
    page_url            VARCHAR(1000),
    product_id          VARCHAR(50),
    event_timestamp     TIMESTAMP_NTZ,
    device_type         VARCHAR(50),
    browser             VARCHAR(100),
    referrer            VARCHAR(500),
    raw_json            VARIANT,
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STG_REVIEWS (
    review_id           VARCHAR(50),
    product_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    rating              INTEGER,
    review_title        VARCHAR(500),
    review_body         VARCHAR(4000),
    review_date         DATE,
    helpful_votes       INTEGER,
    verified_purchase   BOOLEAN,
    _load_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
);

-- ─── COPY INTO TEMPLATES ───────────────────────────────────────────────────
-- COPY INTO STG_CUSTOMERS FROM @stg_customers_stage/customers.csv;
-- COPY INTO STG_PRODUCTS  FROM @stg_products_stage/products.csv;
-- COPY INTO STG_ORDERS    FROM @stg_orders_stage/orders.csv;
-- COPY INTO STG_PAYMENTS  FROM @stg_payments_stage/payments.csv;
-- COPY INTO STG_REVIEWS   FROM @stg_reviews_stage/reviews.csv;
